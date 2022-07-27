package acceptance

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/rs/xid"
)

func Test_Columnar(t *testing.T) {
	containerName := fmt.Sprintf("spilo-%s", xid.New())

	go func() {
		cmd := newCmd(
			"docker",
			"run",
			"--rm",
			"--name",
			containerName,
			"-e",
			"PGVERSION=13",
			"-e",
			"SPILO_PROVIDER=local",
			"-p",
			"127.0.0.1:5432:5432",
			flagSpiloImage,
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

	var (
		ctx  = context.Background()
		conn *pgx.Conn
	)

	waitUntil(t, 8, func() error {
		var err error
		conn, err = pgx.Connect(ctx, "postgres://postgres:zalando@127.0.0.1:5432")
		if err != nil {
			return err
		}

		return nil
	})
	defer conn.Close(ctx)

	if err := conn.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		Name string
		SQL  string
	}{
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

	for _, c := range cases {
		c := c
		t.Run(c.Name, func(t *testing.T) {
			if _, err := conn.Exec(ctx, c.SQL); err != nil {
				t.Fatal(err)
			}
		})
	}
}
