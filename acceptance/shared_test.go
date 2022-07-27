package acceptance

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
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

func terminateContainer(name string) error {
	cmd := exec.Command(
		"docker",
		"ps",
		"-a",
		"-q",
		"-f",
		"name="+name,
	)

	log.Println(cmd.String())
	b, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("Error finding Docker process for name %q: %s %w", name, b, err)
	}

	if containerID := string(b); containerID != "" {
		stopCmd := newCmd("docker", "stop", "-t", "1", strings.TrimSpace(containerID))
		log.Println(stopCmd.String())
		if err := stopCmd.Run(); err != nil {
			return err
		}

		waitCmd := newCmd("docker", "wait", strings.TrimSpace(containerID))
		log.Println(waitCmd.String())
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
