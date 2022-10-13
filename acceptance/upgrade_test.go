package acceptance

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func Test_HydraUpgrade(t *testing.T) {
	type Case struct {
		Name        string
		BeforeImage string
		AfterImage  string
	}

	cases := []Case{
		{
			Name:        "upgrade from columnar ext rename",
			BeforeImage: "ghcr.io/hydrasdb/hydra:7149160_8eee2a8_dacf51c",
			AfterImage:  flagHydraImage,
		},
		{
			Name:        "upgrade from the current latest",
			BeforeImage: "ghcr.io/hydrasdb/hydra:latest",
			AfterImage:  flagHydraImage,
		},
	}

	for i, c := range cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			tmpdir, err := os.MkdirTemp("", "")
			if err != nil {
				t.Fatal(err)
			}
			pgdata := filepath.Join(tmpdir, "data")

			container := Container{
				Name:            "hydra",
				Image:           c.BeforeImage,
				Port:            35433 + i,
				ReadinessPort:   38009 + i,
				MountDataVolume: pgdata,
			}

			// run hydra with before image
			cancel := runHydraContainer(t, container)

			pool1 := newPGPool(t, container)
			q1 := `
CREATE TABLE my_columnar_table
(
    id INT,
    i1 INT,
    i2 INT8,
    n NUMERIC,
    t TEXT
) USING columnar;
			`
			if _, err := pool1.Exec(context.Background(), q1); err != nil {
				t.Fatal(err)
			}

			// kill hydra
			cancel()

			// run hydra with after image
			container.Image = c.AfterImage
			cancel = runHydraContainer(t, container)
			defer cancel()

			pool2 := newPGPool(t, container)
			q2 := `
CREATE TABLE my_columnar_table2
(
    id INT,
    i1 INT,
    i2 INT8,
    n NUMERIC,
    t TEXT
) USING columnar;
			`
			if _, err := pool2.Exec(context.Background(), q2); err != nil {
				t.Fatal(err)
			}
		})
	}

}
