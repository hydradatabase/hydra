package spilo_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/hydradatabase/hydra/acceptance/shared"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joeshaw/envdecode"
	"github.com/rs/xid"
)

type dockerComposeData struct {
	Image               string
	PostgresVersion     string
	PostgresUser        string
	PostgresPassword    string
	PostgresPort        int
	ReadinessPort       int
	MySQLFixtureSQLPath string
	StartEverything     bool
}

var (
	dockerComposeTmpl = template.Must(template.New("docker-compose.yml").Parse(`
version: "3.9"
services:
  hydra:
    image: {{ .Image }}
    {{- if .StartEverything }}
    depends_on:
      mysql:
        condition: service_healthy
    {{- end }}
    environment:
      PGUSER_SUPERUSER: {{ .PostgresUser }}
      PGPASSWORD_SUPERUSER: {{ .PostgresPassword }}
      PGVERSION: {{ .PostgresVersion }}
      PGROOT: /home/postgres/pgroot
      PGDATA: /home/postgres/pgroot/pgdata
    ports:
      - "{{ .PostgresPort }}:5432"
      - "{{ .ReadinessPort }}:8008"
    volumes:
      - pg_data:/home/postgres/pgroot/pgdata
{{- if .StartEverything }}
  mysql:
    image: mysql:8.0.31
    environment:
      MYSQL_USER: mysql
      MYSQL_PASSWORD: mysql
      MYSQL_ROOT_PASSWORD: mysql
    {{- if .MySQLFixtureSQLPath }}
    volumes:
      - {{ .MySQLFixtureSQLPath }}:/docker-entrypoint-initdb.d/mysql.sql
    {{- end}}
    ports:
      - "3306:3306"
    healthcheck:
      test: ["CMD", "mysqladmin" ,"ping", "-h", "localhost"]
      timeout: 10s
      retries: 5
{{- end }}
volumes:
  pg_data:
`))
)

type Config struct {
	Image                string        `env:"SPILO_IMAGE,required"`
	UpgradeFromImage     string        `env:"SPILO_UPGRADE_FROM_IMAGE,required"`
	ArtifactDir          string        `env:"ARTIFACT_DIR,default="`
	WaitForStartTimeout  time.Duration `env:"WAIT_FOR_START_TIMEOUT,default=60s"`
	WaitForStartInterval time.Duration `env:"WAIT_FOR_START_INTERVAL,default=5s"`
	PostgresVersion      string        `env:"SPILO_POSTGRES_VERSION,default=13"`
	PostgresPort         int           `env:"POSTGRES_PORT,default=5432"`
	ReadinessPort        int           `env:"READINESS_PORT,default=8008"`
}

var config Config

const (
	pgusername = "postgres"
	pgpassword = "hydra"
)

func TestMain(m *testing.M) {
	if err := envdecode.StrictDecode(&config); err != nil {
		log.Fatal(err)
	}

	shared.MustHaveValidArtifactDir(config.ArtifactDir)

	os.Exit(m.Run())
}

type spiloAcceptanceCompose struct {
	config Config

	project string
	pool    *pgxpool.Pool
}

func (c *spiloAcceptanceCompose) StartCompose(t *testing.T, ctx context.Context, img string, startEverything bool) {
	// Only set the project on first start
	if c.project == "" {
		c.project = fmt.Sprintf("spilo-%s", xid.New())
	}

	pwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	dockerCompose := bytes.NewBuffer(nil)
	if err := dockerComposeTmpl.Execute(dockerCompose, dockerComposeData{
		Image:               img,
		PostgresVersion:     c.config.PostgresVersion,
		PostgresUser:        pgusername,
		PostgresPassword:    pgpassword,
		PostgresPort:        c.config.PostgresPort,
		ReadinessPort:       c.config.ReadinessPort,
		StartEverything:     startEverything,
		MySQLFixtureSQLPath: filepath.Join(pwd, "..", "fixtures", "mysql.sql"),
	}); err != nil {
		t.Fatal(err)
	}

	// ArtifactDir may be empty, in which case the system tmp directory is used
	f, err := os.CreateTemp(c.config.ArtifactDir, "docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := f.WriteString(dockerCompose.String()); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	runCmd := exec.CommandContext(ctx, "docker", "compose", "--project-name", c.project, "--file", f.Name(), "up", "--detach")

	t.Logf("Starting docker compose %s with %s", c.project, f.Name())
	if o, err := runCmd.CombinedOutput(); err != nil {
		t.Fatalf("unable to start docker compose %s: %s", err, o)
		return
	}

	c.WaitForContainerReady(t, ctx)
}

func (c *spiloAcceptanceCompose) WaitForContainerReady(t *testing.T, ctx context.Context) {
	done := make(chan bool, 1)
	timeout := time.After(c.config.WaitForStartTimeout)
	ticker := time.NewTicker(c.config.WaitForStartInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://127.0.0.1:%d", c.config.ReadinessPort), nil)
			if err != nil {
				t.Fatal(err)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Logf("waiting for spilo to be ready: %s", err)
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Logf("unable to read readiness body: %s", err)
					continue
				}

				t.Logf("waiting for spilo to be ready %d: %s", resp.StatusCode, body)
				continue
			}

			pool, err := shared.CreatePGPool(t, ctx, pgusername, pgpassword, c.config.PostgresPort)
			if err != nil {
				t.Fatalf("unable to create PG Pool: %s", err)
			}

			c.pool = pool
			done <- true
		case <-timeout:
			t.Fatalf("timed out waiting for container to start after %s", c.config.WaitForStartTimeout)
		}
	}
}

func (c spiloAcceptanceCompose) TerminateCompose(t *testing.T, ctx context.Context, kill bool) {
	shared.TerminateDockerComposeProject(t, ctx, c.project, c.config.ArtifactDir, kill)
}

func (c spiloAcceptanceCompose) Image() string {
	return c.config.Image
}

func (c spiloAcceptanceCompose) UpgradeFromImage() string {
	return c.config.UpgradeFromImage
}

func (c spiloAcceptanceCompose) PGPool() *pgxpool.Pool {
	return c.pool
}

func Test_SpiloAcceptance(t *testing.T) {
	shared.RunAcceptanceTests(
		t,
		context.Background(),
		&spiloAcceptanceCompose{config: config},
		shared.Case{
			Name: "no timescaledb ext",
			SQL: `
SELECT count(1) FROM pg_available_extensions WHERE name = 'timescaledb';
			`,
			Validate: func(t *testing.T, row pgx.Row) {
				var count int
				if err := row.Scan(&count); err != nil {
					t.Fatal(err)
				}

				if want, got := 0, count; want != got {
					t.Errorf("timescaledb ext should not exist")
				}
			},
		},
		shared.Case{
			Name: "cron should use worker processes",
			SQL:  `SHOW cron.use_background_workers;`,
			Validate: func(t *testing.T, row pgx.Row) {
				var settingValue string
				if err := row.Scan(&settingValue); err != nil {
					t.Fatal(err)
				}

				if want, got := "on", settingValue; want != got {
					t.Errorf("cron.use_background_workers not set to 'on'")
				}
			},
		},
		shared.Case{
			Name: fmt.Sprintf("spilo started the expected postgres version %s", config.PostgresVersion),
			SQL:  `SHOW server_version;`,
			Validate: func(t *testing.T, row pgx.Row) {
				var version string
				if err := row.Scan(&version); err != nil {
					t.Fatal(err)
				}

				if !strings.HasPrefix(version, config.PostgresVersion) {
					t.Errorf("incorrect postgres version, got %s, expected major version %s", version, config.PostgresVersion)
				}
			},
		},
	)
}

func Test_SpiloUpgrade(t *testing.T) {
	c := spiloAcceptanceCompose{
		config: config,
	}

	shared.RunUpgradeTests(t, context.Background(), &c)
}
