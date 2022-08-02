package acceptance

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/rs/xid"
)

type Container struct {
	Name  string
	Image string
	Port  int
}

func Test_Hydra(t *testing.T) {
	containers := []Container{
		{
			Name:  "hydra",
			Image: flagHydraImage,
			Port:  35432,
		},
		{
			Name:  "hydra-all",
			Image: flagHydraAllImage,
			Port:  45432,
		},
	}

	for _, c := range containers {
		c := c

		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()

			testHydra(t, c)
		})
	}
}

func testHydra(t *testing.T, c Container) {
	containerName := fmt.Sprintf("%s-%s", c.Name, xid.New())

	go func() {
		cmd := newCmd(
			"docker",
			"run",
			"--rm",
			"--name",
			containerName,
			"-p",
			fmt.Sprintf("127.0.0.1:%d:5432", c.Port),
			c.Image,
		)
		log.Println(cmd.String())
		if err := cmd.Run(); err != nil {
			log.Println(err)
		}
	}()
	defer func() {
		if err := terminateContainer(containerName); err != nil {
			log.Println(err)
		}
	}()

	t.Log("Waiting for containers to fully spawn")
	time.Sleep(10 * time.Second)

	var (
		ctx  = context.Background()
		pool *pgxpool.Pool
	)

	waitUntil(t, 8, func() error {
		var err error
		pool, err = pgxpool.Connect(ctx, fmt.Sprintf("postgres://postgres:hydra@127.0.0.1:%d", c.Port))
		if err != nil {
			return err
		}

		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			return err
		}

		return nil
	})
	defer pool.Close()

	type Case struct {
		Name     string
		SQL      string
		Validate func(t *testing.T, row pgx.Row)
	}

	cases := []Case{
		{
			Name: "columnar ext",
			SQL: `
SELECT count(1) FROM pg_available_extensions WHERE name = 'citus_columnar';
			`,
			Validate: func(t *testing.T, row pgx.Row) {
				var count int
				if err := row.Scan(&count); err != nil {
					t.Fatal(err)
				}

				if want, got := 1, count; want != got {
					t.Fatalf("columnar ext should exist")
				}
			},
		},
		{
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
					t.Fatalf("timescaledb ext should not exist")
				}
			},
		},
		{
			Name: "using a columnar table",
			SQL: `
CREATE TABLE my_columnar_table
(
    id INT,
    i1 INT,
    i2 INT8,
    n NUMERIC,
    t TEXT
) USING columnar;
			`,
		},
		// TODO: this example does not work due to https://linear.app/hydra/issue/HYD-30/cant-change-existing-table-to-columnar
		// {
		// Name: "convert between row and columnar",
		// SQL: `
		// CREATE TABLE my_table(i INT8 DEFAULT '7');
		// INSERT INTO my_table VALUES(1);
		// -- convert to columnar
		// SELECT alter_table_set_access_method('my_table', 'columnar');
		// -- back to row
		// SELECT alter_table_set_access_method('my_table', 'heap');
		// `,
		// },
		{
			Name: "convert by copying",
			SQL: `
CREATE TABLE table_heap (i INT8);
CREATE TABLE table_columnar (LIKE table_heap) USING columnar;
INSERT INTO table_columnar SELECT * FROM table_heap;
			`,
		},
		{
			Name: "partition",
			SQL: `
CREATE TABLE parent(ts timestamptz, i int, n numeric, s text)
  PARTITION BY RANGE (ts);

-- columnar partition
CREATE TABLE p0 PARTITION OF parent
  FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')
  USING COLUMNAR;
-- columnar partition
CREATE TABLE p1 PARTITION OF parent
  FOR VALUES FROM ('2020-02-01') TO ('2020-03-01')
  USING COLUMNAR;
-- row partition
CREATE TABLE p2 PARTITION OF parent
  FOR VALUES FROM ('2020-03-01') TO ('2020-04-01');

INSERT INTO parent VALUES ('2020-01-15', 10, 100, 'one thousand'); -- columnar
INSERT INTO parent VALUES ('2020-02-15', 20, 200, 'two thousand'); -- columnar
INSERT INTO parent VALUES ('2020-03-15', 30, 300, 'three thousand'); -- row

CREATE INDEX p2_ts_idx ON p2 (ts);
CREATE UNIQUE INDEX p2_i_unique ON p2 (i);
ALTER TABLE p2 ADD UNIQUE (n);
			`,
		},
		{
			Name: "options",
			SQL: `
SELECT alter_columnar_table_set(
    'my_columnar_table',
    compression => 'none',
    stripe_row_limit => 10000);
			`,
		},
	}

	if c.Name == "hydra-all" {
		cases = append(cases, Case{
			Name: "hydra ext",
			SQL: `
SELECT count(1) FROM pg_available_extensions WHERE name = 'hydra';
			`,
			Validate: func(t *testing.T, row pgx.Row) {
				var count int
				if err := row.Scan(&count); err != nil {
					t.Fatal(err)
				}

				if want, got := 1, count; want != got {
					t.Fatalf("hydra ext should exist")
				}
			},
		})
	} else {
		cases = append(cases, Case{
			Name: "no hydra ext",
			SQL: `
SELECT count(1) FROM pg_available_extensions WHERE name = 'hydra';
			`,
			Validate: func(t *testing.T, row pgx.Row) {
				var count int
				if err := row.Scan(&count); err != nil {
					t.Fatal(err)
				}

				if want, got := 0, count; want != got {
					t.Fatalf("hydra ext should not exist")
				}
			},
		})

	}

	for _, c := range cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			v := c.Validate
			if v == nil {
				if _, err := pool.Exec(ctx, c.SQL); err != nil {
					t.Fatal(err)
				}
			} else {
				rows := pool.QueryRow(ctx, c.SQL)
				v(t, rows)
			}
		})
	}
}
