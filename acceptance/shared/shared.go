// Package shared implements utility functions for acceptance testing Hydra as
// well as shared test cases.
package shared

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrPgPoolConnect is used when pgxpool cannot connect to a database.
var ErrPgPoolConnect = errors.New("pgxpool did not connect")

// MustHaveValidArtifactDir ensures that if a artifact directory is
// present it is has an absolute path as go tests cannot determine the directory
// that they are running from.
func MustHaveValidArtifactDir(dir string) {
	if dir != "" && !filepath.IsAbs(dir) {
		log.Fatalf("the artifact dir must be absolute, got %s", dir)
	}
}

// CreatePGPool calls pgxpool.New and then sends a Ping to the database to
// ensure it is running. If the ping fails it returns a wrapped
// ErrPgPoolConnect.
func CreatePGPool(t *testing.T, ctx context.Context, username, password string, port int) (*pgxpool.Pool, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d", username, password, port))
	if err != nil {
		return nil, fmt.Errorf("failed to construct new pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrPgPoolConnect, err)
	}

	t.Cleanup(func() {
		pool.Close()
	})

	return pool, nil
}

// TerminateDockerComposeProject terminates a running docker compose project. If logDir is
// included then the container logs are saved to that directory before it is
// terminated. If killAndCleanup is false docker stop is used, otherwise docker compose
// kill is used and volumes are also deleted.
func TerminateDockerComposeProject(t *testing.T, ctx context.Context, project, logDir string, killAndCleanup bool) {
	if project == "" {
		return
	}

	writeDockerComposeLogs(t, ctx, project, logDir)

	if killAndCleanup {
		termCmd := exec.CommandContext(ctx, "docker", "compose", "--project-name", project, "kill")
		if output, err := termCmd.CombinedOutput(); err != nil {
			t.Fatalf("unable to terminate docker compose %s: %s", err, output)
		}
	}

	// always run docker compose down to clean up the containers and network
	downCmdConfig := []string{"compose", "--project-name", project, "down", "--timeout", "30"}
	if killAndCleanup {
		// but only remove the volumes if killing the containers
		downCmdConfig = append(downCmdConfig, "--volumes")
	}

	downCmd := exec.CommandContext(ctx, "docker", downCmdConfig...)
	if output, err := downCmd.CombinedOutput(); err != nil {
		t.Fatalf("unable to stop docker compose %s: %s", err, output)
	}
}

func writeDockerComposeLogs(t *testing.T, ctx context.Context, project, logDir string) {
	if logDir == "" {
		return
	}

	logCmd := exec.CommandContext(ctx, "docker", "compose", "--project-name", project, "logs", "--no-color", "hydra")
	logOutput, err := logCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("unable to fetch docker compose log %s: %s", err, logOutput)
	}

	if err := os.WriteFile(filepath.Join(logDir, fmt.Sprintf("%s-%s.log", project, time.Now().Format(time.RFC3339))), logOutput, 0644); err != nil {
		t.Fatalf("unable to write docker compose log: %s", err)
	}
}
