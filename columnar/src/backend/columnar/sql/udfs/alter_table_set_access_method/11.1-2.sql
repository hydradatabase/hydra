CREATE OR REPLACE FUNCTION columnar.alter_table_set_access_method(t TEXT, method TEXT)
  RETURNS BOOLEAN LANGUAGE plpgsql
AS $func$

DECLARE

    tbl_exists BOOLEAN;
    tbl_schema TEXT = 'public';
    tbl_name TEXT;
    tbl_array TEXT[] = (parse_ident(t));
    tbl_oid INT;
    temp_tbl_name TEXT;

    trigger_list_definition TEXT[];
    trigger TEXT;

    index_list_definition TEXT[];
    idx TEXT;

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

    -- Allow only convert to columnar access method

    IF method NOT IN ('columnar') THEN
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

    -- Get table OID

    EXECUTE FORMAT('SELECT  %L::regclass::oid'::text, tbl_schema || '.' || tbl_name) INTO tbl_oid;

    -- Check if table has FOREIGN KEY

    IF (SELECT COUNT(1) FROM pg_constraint WHERE contype = 'f' AND conrelid = tbl_oid) THEN
        RAISE WARNING 'Cannot convert table: table %.% has a FOREIGN KEY constraint.', tbl_schema, tbl_name;
        RETURN 0;
    END IF;

    -- Check if table is REFERENCED by FOREIGN KEY

    IF (SELECT COUNT(1) FROM pg_constraint WHERE contype = 'f' AND confrelid = tbl_oid) THEN
        RAISE WARNING 'Cannot convert table: table %.% is referenced by FOREIGN KEY.', tbl_schema, tbl_name;
        RETURN 0;
    END IF;

    -- Check if table has identity columns

    IF (SELECT COUNT(1) FROM pg_attribute WHERE attrelid = tbl_oid AND attidentity <> '') THEN
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

            schemaname = tbl_schema AND tablename = tbl_name

            AND

            indexname::regclass::oid IN 
                ( 
                    SELECT indexrelid FROM pg_index 

                    WHERE
                        indexrelid IN 
                            (SELECT indexname::regclass::oid FROM pg_indexes
                                WHERE schemaname = tbl_schema AND tablename = tbl_name)

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
        CREATE TABLE %I (LIKE %I.%I 
                         INCLUDING GENERATED
                         INCLUDING DEFAULTS
        ) USING columnar'::text, temp_tbl_name, tbl_schema, tbl_name);

    -- Insert all data from original table

    EXECUTE FORMAT('INSERT INTO %I SELECT * FROM %I.%I'::text, temp_tbl_name, tbl_schema, tbl_name);

    -- Drop original table

    EXECUTE FORMAT('DROP TABLE %I'::text, tbl_name);

    -- Rename new table to original name

    EXECUTE FORMAT('ALTER TABLE %I RENAME TO %I;'::text, temp_tbl_name, tbl_name);

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

    RETURN 1;

END;

$func$;

COMMENT ON FUNCTION columnar.alter_table_set_access_method(t text, method text)
  IS 'alters a table''s access method';