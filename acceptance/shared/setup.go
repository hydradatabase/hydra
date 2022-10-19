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

var ErrPgPoolConnect = errors.New("pgpool did not connect")

func MustHaveValidContainerLogDir(logDir string) {
	if logDir != "" && !filepath.IsAbs(logDir) {
		log.Fatalf("the container log dir must be absolute, got %s", logDir)
	}
}

func CreatePGPool(t *testing.T, ctx context.Context, username, password string, port int) (*pgxpool.Pool, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d", username, password, port))
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrPgPoolConnect, err)
	}

	t.Cleanup(func() {
		pool.Close()
	})

	return pool, nil
}

func TerminateContainer(t *testing.T, ctx context.Context, containerName, logDir string, kill bool) {
	if containerName == "" {
		return
	}

	WriteLogs(t, ctx, containerName, logDir)

	var termCmd *exec.Cmd
	if kill {
		termCmd = exec.CommandContext(ctx, "docker", "kill", containerName)
	} else {
		termCmd = exec.CommandContext(ctx, "docker", "stop", "--time", "30", containerName)
	}

	if output, err := termCmd.CombinedOutput(); err != nil {
		t.Fatalf("unable to terminate container %s: %s", err, output)
	}
}

func WriteLogs(t *testing.T, ctx context.Context, containerName, logDir string) {
	if logDir == "" {
		return
	}

	logCmd := exec.CommandContext(ctx, "docker", "logs", containerName)
	logOutput, err := logCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("unable to fetch container log %s: %s", err, logOutput)
	}

	if err := os.WriteFile(filepath.Join(logDir, fmt.Sprintf("%s.log", containerName)), logOutput, 0644); err != nil {
		t.Fatalf("unable to write container log: %s", err)
	}
}
