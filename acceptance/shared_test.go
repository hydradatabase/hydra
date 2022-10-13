package acceptance

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/xid"
)

func waitUntil(tb testing.TB, times int, fn func() error) {
	tb.Helper()

	var err error
	for tries := 0; tries < times; tries++ {
		err = fn()
		if err == nil {
			return
		}

		time.Sleep(time.Duration(tries*tries) * time.Second)
	}

	tb.Fatal(err)
}

func terminateContainer(t *testing.T, name string) error {
	cmd := exec.Command(
		"docker",
		"ps",
		"-a",
		"-q",
		"-f",
		"name="+name,
	)

	t.Log(cmd.String())
	b, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("Error finding Docker process for name %q: %s %w", name, b, err)
	}

	if containerID := string(b); containerID != "" {
		stopCmd := newCmd("docker", "stop", "-t", "1", strings.TrimSpace(containerID))
		t.Log(stopCmd.String())
		if err := stopCmd.Run(); err != nil {
			return err
		}

		waitCmd := newCmd("docker", "wait", strings.TrimSpace(containerID))
		t.Log(waitCmd.String())
		if err := waitCmd.Run(); err != nil {
			return err
		}
	}

	return nil
}

func newCmd(name string, arg ...string) *exec.Cmd {
	cmd := exec.Command(
		name,
		arg...,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd
}

func runHydraContainer(t *testing.T, c Container) (cancel func()) {
	t.Helper()

	containerName := fmt.Sprintf("%s-%s", c.Name, xid.New())

	go func() {
		cmdStr := []string{
			"docker",
			"run",
			"--rm",
			"--name",
			containerName,
			"-p",
			fmt.Sprintf("127.0.0.1:%d:5432", c.Port),
			"-p",
			fmt.Sprintf("127.0.0.1:%d:8008", c.ReadinessPort),
		}
		if v := c.MountDataVolume; v != "" {
			cmdStr = append(
				cmdStr,
				"-v",
				fmt.Sprintf("%s:/home/postgres/pgdata", v),
			)
		}
		cmdStr = append(
			cmdStr,
			c.Image,
		)

		cmd := newCmd(cmdStr[0], cmdStr[1:]...)
		t.Log(cmd.String())

		if err := cmd.Run(); err != nil {
			t.Log(err)
		}
	}()

	waitUntil(t, 8, func() error {
		t.Log("Waiting for containers to fully spawn")
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d", c.ReadinessPort))
		if err != nil {
			return err
		}

		if resp.StatusCode != 200 {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}

			return fmt.Errorf("db is not ready: code=%d, body=%s", resp.StatusCode, body)
		}

		return nil
	})

	return func() {
		if err := terminateContainer(t, containerName); err != nil {
			t.Log(err)
		}
	}
}

func newPGPool(t *testing.T, c Container) *pgxpool.Pool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.Connect(ctx, fmt.Sprintf("postgres://postgres:hydra@127.0.0.1:%d", c.Port))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
	})

	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	return pool
}
