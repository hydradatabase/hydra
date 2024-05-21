CREATE OR REPLACE FUNCTION columnar.alter_table_set_access_method(t TEXT, method TEXT)
  RETURNS BOOLEAN LANGUAGE plpgsql
AS $func$

DECLARE

    tbl_exists BOOLEAN;
    tbl_schema TEXT = 'public';
    tbl_name TEXT;
    tbl_array TEXT[] = (parse_ident(t));
    tbl_oid INT;
    tbl_am_oid INT;
    temp_tbl_name TEXT;

    is_case_sensitive BOOLEAN;
    tbl_name_original TEXT;
    tbl_schema_original TEXT;

    trigger_list_definition TEXT[];
    trigger TEXT;

    index_list_definition TEXT[];
    idx TEXT;

    saved_search_path TEXT;

    constraint_list_name_and_definition TEXT[];
    constraint_name_and_definition TEXT;
    constraint_name_and_definition_split TEXT[];

BEGIN
    CASE 
        WHEN CARDINALITY(tbl_array) = 1 THEN 
            SELECT tbl_array[1] INTO tbl_name;
        WHEN CARDINALITY(tbl_array) = 2 THEN 
            SELECT tbl_array[1] INTO tbl_schema;
            SELECT tbl_array[2] INTO tbl_name;
        ELSE 
            RAISE WARNING 'Argument should provided as table or schema.table.';
            RETURN 0;
    END CASE;

    -- Allow only convert to columnar / heap access method

    IF method NOT IN ('columnar', 'heap') THEN
        RAISE WARNING 'Cannot convert table: Allowed access methods are heap and columnar.';
        RETURN 0;
    END IF;

    -- Check if table exists

    SELECT EXISTS 
        (SELECT FROM pg_catalog.pg_tables WHERE  schemaname = tbl_schema AND tablename  = tbl_name)
    INTO tbl_exists;

    IF tbl_exists = False THEN
        RAISE WARNING 'Table %.% does not exist.', tbl_schema, tbl_name;
        RETURN 0;
    END IF;

    -- Case senstivitiy

    SELECT EXISTS (SELECT regexp_matches(tbl_name,'[A-Z]')) INTO is_case_sensitive;

    SELECT tbl_name INTO tbl_name_original;

    IF is_case_sensitive = True THEN
        SELECT quote_ident(tbl_name) INTO tbl_name;
    END IF;

    SELECT EXISTS (SELECT regexp_matches(tbl_schema,'[A-Z]')) INTO is_case_sensitive;

    SELECT tbl_schema INTO tbl_schema_original;

    IF is_case_sensitive = True THEN
        SELECT quote_ident(tbl_schema) INTO tbl_schema;
    END IF;

    SELECT current_setting('search_path') INTO saved_search_path;

    EXECUTE FORMAT('SET search_path TO %s'::text, tbl_schema);

    -- Get table OID

    EXECUTE FORMAT('SELECT  %L::regclass::oid'::text, tbl_schema || '.' || tbl_name) INTO tbl_oid;

    -- Get table AM oid

    SELECT relam FROM pg_class WHERE oid = tbl_oid INTO tbl_am_oid;

    -- Check that table is heap or columnar

    IF (tbl_am_oid != (SELECT oid FROM pg_am WHERE amname = 'columnar')) AND
       (tbl_am_oid != (SELECT oid FROM pg_am WHERE amname = 'heap')) THEN
        RAISE WARNING 'Cannot convert table: table %.% is not heap or colummnar', tbl_schema, tbl_name;
        RETURN 0;
    END IF;

    -- Check that we can convert only from 'heap' to 'columnar' and vice versa

    IF tbl_am_oid = (SELECT oid FROM pg_am WHERE amname = method) THEN
        RAISE WARNING 'Cannot convert table: conversion to same access method.';
        RETURN 0;
    END IF;

    -- Check if table has FOREIGN KEY

    IF (SELECT COUNT(1) FROM pg_constraint WHERE contype = 'f' AND conrelid = tbl_oid) > 0 THEN
        RAISE WARNING 'Cannot convert table: table %.% has a FOREIGN KEY constraint.', tbl_schema, tbl_name;
        RETURN 0;
    END IF;

    -- Check if table is REFERENCED by FOREIGN KEY

    IF (SELECT COUNT(1) FROM pg_constraint WHERE contype = 'f' AND confrelid = tbl_oid) > 0 THEN
        RAISE WARNING 'Cannot convert table: table %.% is referenced by FOREIGN KEY.', tbl_schema, tbl_name;
        RETURN 0;
    END IF;

    -- Check if table has identity columns

    IF (SELECT COUNT(1) FROM pg_attribute WHERE attrelid = tbl_oid AND attidentity <> '') > 0 THEN
        RAISE WARNING 'Cannot convert table: table %.% must not use GENERATED ... AS IDENTITY.', tbl_schema, tbl_name;
        RETURN 0;
    END IF;

    -- Collect triggers definitions

    SELECT ARRAY_AGG(pg_get_triggerdef(oid)) FROM pg_trigger 
        WHERE tgrelid = tbl_oid INTO trigger_list_definition;

    -- Collect constraint names and definitions (delimiter is `?`)
    -- Search for constraints that depend on index AM which is supported by columnar AM

     SELECT ARRAY_AGG(pg_constraint.conname || '?' || pg_get_constraintdef(pg_constraint.oid))

        FROM pg_constraint, pg_class 
        
        WHERE 
            pg_constraint.conindid = pg_class.oid 
            AND
            pg_constraint.conrelid = tbl_oid
            AND
            pg_class.relam IN (SELECT oid FROM pg_am WHERE amname IN ('btree', 'hash'))

        INTO constraint_list_name_and_definition;

    -- Collect index definitions which are not constraints

    SELECT ARRAY_AGG(indexdef) FROM pg_indexes

        WHERE 

            schemaname = tbl_schema_original AND tablename = tbl_name_original

            AND

            quote_ident(indexname)::regclass::oid IN 
                ( 
                    SELECT indexrelid FROM pg_index 

                    WHERE
                        indexrelid IN 
                            (SELECT quote_ident(indexname)::regclass::oid FROM pg_indexes
                                WHERE schemaname = tbl_schema_original AND tablename = tbl_name_original)

                        AND

                        indexrelid NOT IN 
                            (SELECT conindid FROM pg_constraint 
                                WHERE pg_constraint.conrelid = tbl_oid)
                )

        INTO index_list_definition;

    -- Generate random name for new table

    SELECT 't_' || substr(md5(random()::text), 0, 25) INTO temp_tbl_name;

    -- Create new table
    
    EXECUTE FORMAT('
        CREATE TABLE %s.%s (LIKE %s.%s 
                         INCLUDING GENERATED
                         INCLUDING DEFAULTS
        ) USING %s'::text, tbl_schema, temp_tbl_name, tbl_schema, tbl_name, method);

    -- Insert all data from original table

    EXECUTE FORMAT('INSERT INTO %s.%s SELECT * FROM %s.%s'::text, tbl_schema, temp_tbl_name, tbl_schema, tbl_name);

    -- Drop original table

    EXECUTE FORMAT('DROP TABLE %s.%s'::text, tbl_schema, tbl_name);

    -- Rename new table to original name

    EXECUTE FORMAT('ALTER TABLE %s.%s RENAME TO %s;'::text, tbl_schema, temp_tbl_name, tbl_name);

    -- Since we inserted rows before they are not flushed so trigger flushing

    EXECUTE FORMAT('SELECT COUNT(1) FROM %s.%s LIMIT 1;'::text, tbl_schema, tbl_name);

    -- Set indexes 

    IF CARDINALITY(index_list_definition) <> 0 THEN
        FOREACH idx IN ARRAY index_list_definition
        LOOP
            BEGIN
                EXECUTE idx;
            EXCEPTION WHEN feature_not_supported THEN 
               RAISE WARNING 'Index `%` cannot be created.', idx;
            END;
        END LOOP;
    END IF;

    -- Set constraints

    IF CARDINALITY(constraint_list_name_and_definition) <> 0 THEN
        FOREACH constraint_name_and_definition IN ARRAY constraint_list_name_and_definition
        LOOP
            SELECT string_to_array(constraint_name_and_definition, '?') INTO constraint_name_and_definition_split;
            BEGIN
                EXECUTE 'ALTER TABLE ' || tbl_name || ' ADD CONSTRAINT ' 
                            || constraint_name_and_definition_split[1] || ' '
                            || constraint_name_and_definition_split[2];
            EXCEPTION WHEN feature_not_supported THEN 
               RAISE WARNING 'Constraint `%` cannot be added.', constraint_name_and_definition_split[2];
             END;
        END LOOP;
    END IF;

    -- Set triggers 

    IF CARDINALITY(trigger_list_definition) <> 0 THEN
        FOREACH trigger IN ARRAY trigger_list_definition
        LOOP
            BEGIN
                EXECUTE trigger;
            EXCEPTION WHEN feature_not_supported THEN 
               RAISE WARNING 'Trigger `%` cannot be applied.', trigger;
               RAISE WARNING 
                'Foreign keys and AFTER ROW triggers are not supported for columnar tables.'
                ' Consider an AFTER STATEMENT trigger instead.';
            END;
        END LOOP;
    END IF;

    -- Restore search_path
    EXECUTE FORMAT('SET search_path TO %s'::text, saved_search_path);

    RETURN 1;

END;

$func$;

COMMENT ON FUNCTION columnar.alter_table_set_access_method(t text, method text)
  IS 'alters a table''s access method';