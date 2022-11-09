package shared

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// A Case describes an acceptance test case. If a Validate function is provided
// then the test will call that function and expect it to handle test failues.
// Otherwise the case will fail if pool.Exec fails on the SQL.
type Case struct {
	Name     string                          // name of the test
	SQL      string                          // SQL to run during the test
	Validate func(t *testing.T, row pgx.Row) // optional validation function
}

// AcceptanceCases describe the shared acceptance criteria for any Hydra-based
// images.
var AcceptanceCases = []Case{
	{
		Name: "columnar ext available",
		SQL: `
SELECT count(1) FROM pg_available_extensions WHERE name = 'columnar';
			`,
		Validate: func(t *testing.T, row pgx.Row) {
			var count int
			if err := row.Scan(&count); err != nil {
				t.Fatal(err)
			}

			if want, got := 1, count; want != got {
				t.Error("columnar ext should exist")
			}
		},
	},
	{
		Name: "columnar ext enabled",
		SQL: `
SELECT count(1) FROM pg_extension WHERE extname = 'columnar';
			`,
		Validate: func(t *testing.T, row pgx.Row) {
			var count int
			if err := row.Scan(&count); err != nil {
				t.Fatal(err)
			}

			if want, got := 1, count; want != got {
				t.Error("columnar ext should exist")
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
	{
		Name: "convert between row and columnar",
		SQL: `
		CREATE TABLE my_table(i INT8 DEFAULT '7');
		INSERT INTO my_table VALUES(1);
		-- convert to columnar
		SELECT columnar.alter_table_set_access_method('my_table', 'columnar');
		-- back to row
		-- TODO: reenable this after it's supported
		-- SELECT alter_table_set_access_method('my_table', 'heap');
		`,
	},
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
SELECT columnar.alter_columnar_table_set(
    'my_columnar_table',
    compression => 'none',
    stripe_row_limit => 10000);
			`,
	},
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
  FROM http_put('http://httpbin.org/put', 'some text', 'text/plain');
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
		Name: "mysql_fdw available",
		SQL: `
SELECT count(1) FROM pg_available_extensions WHERE name = 'mysql_fdw';
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
		Name: "enable mysql_fdw ext",
		SQL: `
CREATE EXTENSION mysql_fdw;
			`,
	},
	{
		Name: "mysql_fdw enabled",
		SQL: `
SELECT count(1) FROM pg_extension WHERE extname = 'mysql_fdw';
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
		Name: "create mysql_fdw foreign table",
		SQL: `
CREATE SERVER mysql_server
	FOREIGN DATA WRAPPER mysql_fdw
	OPTIONS (host 'mysql', port '3306');

CREATE USER MAPPING FOR CURRENT_USER
	SERVER mysql_server
	OPTIONS (username 'root', password 'mysql');

CREATE FOREIGN TABLE warehouse
	(
		warehouse_id int,
		warehouse_name text,
		warehouse_created timestamp
	)
	SERVER mysql_server
	OPTIONS (dbname 'test', table_name 'warehouse');
			`,
	},
	{
		Name: "insert data to mysql_fdw foreign table",
		SQL: `
INSERT INTO warehouse values (1, 'UPS', current_date);
INSERT INTO warehouse values (2, 'TV', current_date);
INSERT INTO warehouse values (3, 'Table', current_date);
		`,
	},
	{
		Name: "validate mysql_fdw foreign table",
		SQL: `
SELECT * FROM warehouse ORDER BY warehouse_id LIMIT 1;
		`,
		Validate: func(t *testing.T, row pgx.Row) {
			var result struct {
				WarehouseID      int
				WarehouseName    string
				WarehouseCreated time.Time
			}
			if err := row.Scan(&result.WarehouseID, &result.WarehouseName, &result.WarehouseCreated); err != nil {
				t.Fatal(err)
			}

			if want, got := 1, result.WarehouseID; want != got {
				t.Errorf("warehouse ID should equal")
			}
			if want, got := "UPS", result.WarehouseName; want != got {
				t.Errorf("warehouse name should equal")
			}
			if result.WarehouseCreated.IsZero() {
				t.Errorf("warehouse created time should not be zero value")
			}
		},
	},
	{
		Name: "multicorn ext available",
		SQL: `
SELECT count(1) FROM pg_available_extensions WHERE name = 'multicorn';
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
		Name: "enable multicorn ext",
		SQL: `
CREATE EXTENSION multicorn;
			`,
	},
	{
		Name: "multicorn ext enabled",
		SQL: `
SELECT count(1) FROM pg_extension WHERE extname = 'multicorn';
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
		Name: "create multicorn s3 ext foreign table",
		SQL: `
CREATE SERVER multicorn_s3 FOREIGN DATA WRAPPER multicorn
options (
  wrapper 's3fdw.s3fdw.S3Fdw'
);

create foreign table s3 (
  id int,
  name text
) server multicorn_s3 options (
  aws_access_key 'FAKE',
  aws_secret_key 'FAKE',
  bucket 'test-bucket',
  filename 'test.csv'
);
		`,
	},
	{
		Name: "create multicorn gspreadsheet ext foreign table",
		SQL: `
CREATE SERVER multicorn_gspreadsheet FOREIGN DATA WRAPPER multicorn
options (
  wrapper 'gspreadsheet_fdw.GspreadsheetFdw' );

CREATE FOREIGN TABLE test_spreadsheet (
  id character varying,
  name   character varying
) server multicorn_gspreadsheet options(
  gskey '1234',
  serviceaccount '{}'
);
		`,
	},
}

// These describe the shared setup and validation cases that occur to validate
// the upgrade between two version of a Hydra-derived image.
var (
	BeforeUpgradeCases = []Case{
		{
			Name: "create columnar table",
			SQL: `
CREATE TABLE columnar_table
(
    id UUID,
    i1 INT,
    i2 INT8,
    n NUMERIC,
    t TEXT
) USING columnar;
		`,
		},
		{
			Name: "insert into columnar table",
			SQL: `
INSERT INTO columnar_table (id, i1, i2, n, t)
VALUES ('75372aac-d74a-4e5a-8bf3-43cdaf9011de', 2, 3, 100.1, 'hydra');
		`,
		},
	}
	AfterUpgradeCases = []Case{
		{
			Name: "create another columnar table",
			SQL: `
CREATE TABLE columnar_table2
(
    id UUID,
    i1 INT,
    i2 INT8,
    n NUMERIC,
    t TEXT
) USING columnar;
		`,
		},
		{
			Name: "validate columnar data",
			SQL:  "SELECT id, i1, i2, n, t FROM columnar_table LIMIT 1;",
			Validate: func(t *testing.T, row pgx.Row) {
				var result struct {
					ID uuid.UUID
					I1 int
					I2 int
					N  float32
					T  string
				}

				if err := row.Scan(&result.ID, &result.I1, &result.I2, &result.N, &result.T); err != nil {
					t.Fatal(err)
				}

				if result.ID != uuid.MustParse("75372aac-d74a-4e5a-8bf3-43cdaf9011de") {
					t.Errorf("id returned %s after upgrade, expected 75372aac-d74a-4e5a-8bf3-43cdaf9011de", result.ID)
				}

				if result.I1 != 2 {
					t.Errorf("i1 returned %d after upgrade, expected 2", result.I1)
				}

				if result.I2 != 3 {
					t.Errorf("i2 returned %d after upgrade, expected 3", result.I2)
				}

				if result.N != 100.1 {
					t.Errorf("n returned %f after upgrade, expected 100.1", result.N)
				}

				if result.T != "hydra" {
					t.Errorf("t returned %s after upgrade, expected hydra", result.T)
				}
			},
		},
	}
)