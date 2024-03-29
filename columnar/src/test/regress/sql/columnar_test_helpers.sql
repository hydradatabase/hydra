CREATE SCHEMA columnar_test_helpers;
SET search_path TO columnar_test_helpers;

CREATE FUNCTION columnar_relation_storageid(relid oid) RETURNS bigint
    LANGUAGE C STABLE STRICT
    AS 'columnar', $$columnar_relation_storageid$$;

CREATE OR REPLACE FUNCTION columnar_storage_info(
    rel regclass,
    version_major OUT int4,
    version_minor OUT int4,
    storage_id OUT int8,
    reserved_stripe_id OUT int8,
    reserved_row_number OUT int8,
    reserved_offset OUT int8)
  STRICT
  LANGUAGE c AS 'columnar', $$columnar_storage_info$$;

CREATE FUNCTION compression_type_supported(type text) RETURNS boolean
AS $$
BEGIN
   EXECUTE 'SET LOCAL columnar.compression TO ' || quote_literal(type);
   return true;
EXCEPTION WHEN invalid_parameter_value THEN
   return false;
END;
$$ LANGUAGE plpgsql;

-- are chunk groups and chunks consistent?
CREATE view chunk_group_consistency AS
WITH a as (
   SELECT storage_id, stripe_num, chunk_group_num, min(value_count) as row_count
   FROM columnar.chunk
   GROUP BY 1,2,3
), b as (
   SELECT storage_id, stripe_num, chunk_group_num, max(value_count) as row_count
   FROM columnar.chunk
   GROUP BY 1,2,3
), c as (
   SELECT storage_id, stripe_num, chunk_group_num, row_count
   FROM columnar.chunk_group
), d as (
   (TABLE a EXCEPT TABLE b) UNION (TABLE b EXCEPT TABLE a) UNION
   (TABLE a EXCEPT TABLE c) UNION (TABLE c EXCEPT TABLE a)
), e as (
   SELECT storage_id, stripe_num, count(*) as chunk_group_count
   FROM columnar.chunk_group
   GROUP BY 1,2
), f as (
   SELECT storage_id, stripe_num, chunk_group_count
   FROM columnar.stripe
), g as (
   (TABLE e EXCEPT TABLE f) UNION (TABLE f EXCEPT TABLE e)
)
SELECT (SELECT count(*) = 0 FROM d) AND
       (SELECT count(*) = 0 FROM g) as consistent;

CREATE FUNCTION columnar_metadata_has_storage_id(input_storage_id bigint) RETURNS boolean
AS $$
DECLARE
   union_storage_id_count integer;
BEGIN
   SELECT count(*) INTO union_storage_id_count FROM
   (
   SELECT storage_id FROM columnar.stripe UNION ALL
   SELECT storage_id FROM columnar.chunk UNION ALL
   SELECT storage_id FROM columnar.chunk_group
   ) AS union_storage_id
   WHERE storage_id=input_storage_id;

   IF union_storage_id_count > 0 THEN
   RETURN true;
   END IF;

   RETURN false;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION columnar_store_memory_stats(
                  OUT TopMemoryContext BIGINT,
		          OUT TopTransactionContext BIGINT,
		          OUT WriteStateContext BIGINT)
    RETURNS RECORD
    LANGUAGE C STRICT VOLATILE
    AS 'columnar', $$columnar_store_memory_stats$$;

CREATE FUNCTION top_memory_context_usage()
	RETURNS BIGINT AS $$
		SELECT TopMemoryContext FROM columnar_test_helpers.columnar_store_memory_stats();
	$$ LANGUAGE SQL VOLATILE;

CREATE OR REPLACE FUNCTION uses_index_scan(command text)
RETURNS BOOLEAN AS $$
DECLARE
  query_plan text;
BEGIN
  FOR query_plan IN EXECUTE 'EXPLAIN' || command LOOP
    IF query_plan ILIKE '%Index Only Scan using%' OR
       query_plan ILIKE '%Index Scan using%'
    THEN
        RETURN true;
    END IF;
  END LOOP;
  RETURN false;
END; $$ language plpgsql;

CREATE OR REPLACE FUNCTION uses_custom_scan(command text)
RETURNS BOOLEAN AS $$
DECLARE
  query_plan text;
BEGIN
  FOR query_plan IN EXECUTE 'EXPLAIN' || command LOOP
    IF query_plan ILIKE '%Custom Scan (ColumnarScan)%'
    THEN
        RETURN true;
    END IF;
  END LOOP;
  RETURN false;
END; $$ language plpgsql;

CREATE OR REPLACE FUNCTION uses_seq_scan(command text)
RETURNS BOOLEAN AS $$
DECLARE
  query_plan text;
BEGIN
  FOR query_plan IN EXECUTE 'EXPLAIN' || command LOOP
    IF query_plan ILIKE '%Seq Scan on %'
    THEN
        RETURN true;
    END IF;
  END LOOP;
  RETURN false;
END; $$ language plpgsql;

CREATE OR REPLACE FUNCTION pg_waitpid(p_pid integer)
RETURNS VOID AS $$
BEGIN
  WHILE EXISTS (SELECT * FROM pg_stat_activity WHERE pid=p_pid)
  LOOP
    PERFORM pg_sleep(0.001);
  END LOOP;
END; $$ language plpgsql;
