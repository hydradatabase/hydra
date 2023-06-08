package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
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
	PostgresUser        string
	PostgresPassword    string
	PostgresPort        int
	StartEverything     bool
	MySQLFixtureSQLPath string
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
      POSTGRES_USER: {{ .PostgresUser }}
      POSTGRES_PASSWORD: {{ .PostgresPassword }}
      PGDATA: /var/lib/postgresql/data/pgdata
    ports:
      - "{{ .PostgresPort }}:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data/pgdata
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
	Image                   string        `env:"POSTGRES_IMAGE,required"`
	UpgradeFromImage        string        `env:"POSTGRES_UPGRADE_FROM_IMAGE,required"`
	ArtifactDir             string        `env:"ARTIFACT_DIR,default="`
	WaitForStartTimeout     time.Duration `env:"WAIT_FOR_START_TIMEOUT,default=15s"`
	WaitForStartInterval    time.Duration `env:"WAIT_FOR_START_INTERVAL,default=2s"`
	PostgresPort            int           `env:"POSTGRES_PORT,default=5432"`
	ExpectedPostgresVersion string        `env:"EXPECTED_POSTGRES_VERSION,required"`
}

var config Config

const (
	pgusername = "hydra"
	pgpassword = "hydra"
)

func TestMain(m *testing.M) {
	if err := envdecode.StrictDecode(&config); err != nil {
		log.Fatal(err)
	}

	shared.MustHaveValidArtifactDir(config.ArtifactDir)

	os.Exit(m.Run())
}

type postgresAcceptanceCompose struct {
	config Config

	project string
	pool    *pgxpool.Pool
}

func (c *postgresAcceptanceCompose) StartCompose(t *testing.T, ctx context.Context, img string, startEverything bool) {
	// Only set the project on first start
	if c.project == "" {
		c.project = fmt.Sprintf("postgres-%s", xid.New())
	}

	pwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	dockerCompose := bytes.NewBuffer(nil)
	if err := dockerComposeTmpl.Execute(dockerCompose, dockerComposeData{
		Image:               img,
		PostgresUser:        pgusername,
		PostgresPassword:    pgpassword,
		PostgresPort:        c.config.PostgresPort,
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

func (c *postgresAcceptanceCompose) WaitForContainerReady(t *testing.T, ctx context.Context) {
	done := make(chan bool, 1)
	timeout := time.After(c.config.WaitForStartTimeout)
	ticker := time.NewTicker(c.config.WaitForStartInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			pool, err := shared.CreatePGPool(t, ctx, pgusername, pgpassword, c.config.PostgresPort)
			if errors.Is(err, shared.ErrPgPoolConnect) {
				continue
			} else if err != nil {
				t.Fatal(err)
			}
			c.pool = pool
			done <- true
		case <-timeout:
			t.Fatalf("timed out waiting for container to start after %s", c.config.WaitForStartTimeout)
		}
	}
}

func (c postgresAcceptanceCompose) TerminateCompose(t *testing.T, ctx context.Context, kill bool) {
	shared.TerminateDockerComposeProject(t, ctx, c.project, c.config.ArtifactDir, kill)
}

func (c postgresAcceptanceCompose) Image() string {
	return c.config.Image
}

func (c postgresAcceptanceCompose) UpgradeFromImage() string {
	return c.config.UpgradeFromImage
}

func (c postgresAcceptanceCompose) PGPool() *pgxpool.Pool {
	return c.pool
}

func Test_PostgresAcceptance(t *testing.T) {
	shared.RunAcceptanceTests(
		t,
		context.Background(),
		&postgresAcceptanceCompose{config: config},
		[]shared.Case{
			// http ext cases are only available to postgres build for now
			// move this back to AcceptanceCases when they are ready
			{
				Name: "http ext available",
				SQL: `
SELECT count(1) FROM pg_available_extensions WHERE name = 'http';
			`,
				Validate: func(t *testing.T, row pgx.Row) {
					var count int
					if err := row.Scan(&count); err != nil {
						t.Fatal(err)
					}

					if want, got := 1, count; want != got {
						t.Errorf("columnar ext should exist")
					}
				},
			},
			{
				Name: "enable http ext",
				SQL: `
CREATE EXTENSION http;
			`,
			},
			{
				Name: "http ext enabled",
				SQL: `
SELECT count(1) FROM pg_extension WHERE extname = 'http';
			`,
				Validate: func(t *testing.T, row pgx.Row) {
					var count int
					if err := row.Scan(&count); err != nil {
						t.Fatal(err)
					}

					if want, got := 1, count; want != got {
						t.Errorf("columnar ext should exist")
					}
				},
			},
			{
				Name: "http put",
				SQL: `
SELECT status, content_type, content::json->>'data' AS data
  FROM http_put('http://httpbun.com/put', 'some text', 'text/plain');
			`,
				Validate: func(t *testing.T, row pgx.Row) {
					var result struct {
						Status      int
						ContentType string
						Data        string
					}

					if err := row.Scan(&result.Status, &result.ContentType, &result.Data); err != nil {
						t.Fatal(err)
					}

					if want, got := 200, result.Status; want != got {
						t.Errorf("http put response status should match: want=%d got=%d", want, got)
					}
					if want, got := "application/json", result.ContentType; want != got {
						t.Errorf("http put response content type should match: want=%s got=%s", want, got)
					}
					if want, got := "some text", result.Data; want != got {
						t.Errorf("http put response data should match: want=%s got=%s", want, got)
					}
				},
			},
			{
				Name: "started with the expected postgres version",
				SQL:  `SHOW server_version;`,
				Validate: func(t *testing.T, row pgx.Row) {
					var version string
					if err := row.Scan(&version); err != nil {
						t.Fatal(err)
					}

					if !strings.HasPrefix(version, config.ExpectedPostgresVersion) {
						t.Errorf("incorrect postgres version, got %s, expected major version %s", version, config.ExpectedPostgresVersion)
					}
				},
			},
		}...,
	)
}

func Test_PostgresUpgrade(t *testing.T) {
	c := postgresAcceptanceCompose{
		config: config,
	}

	shared.RunUpgradeTests(t, context.Background(), &c)
}
