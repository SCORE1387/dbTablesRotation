CREATE OR REPLACE FUNCTION rotate_tables(table_name TEXT)
  RETURNS TEXT AS
  $BODY$
  DECLARE
    rows_count           INT;
    secondary_table_name TEXT;
    history_table_name   TEXT;
    new_table_name       TEXT;
    if_cold_ts_exists    BOOLEAN;
    rows_limit           INT;
  BEGIN
    EXECUTE 'select count(*) from ' || table_name || ';' INTO rows_count;
    SELECT value FROM ems_config WHERE key = 'ems.rotation.maxlosize' INTO rows_limit;
    SELECT EXISTS (SELECT * FROM pg_tablespace where spcname = 'ems_historical_data') INTO if_cold_ts_exists;

    IF rows_limit IS NULL THEN rows_limit := 2000000; END IF;
    IF rows_count < rows_limit THEN RETURN 'Not rotated, rows count = ' || rows_count; END IF;

    secondary_table_name := table_name || '_2';
    history_table_name := table_name || '_' || to_char(sysdate(), 'YYYYMMDD_HH24mmss');
    new_table_name := 'NEW_' || table_name;

    EXECUTE 'create table ' || new_table_name || ' (like ' || table_name || ' INCLUDING ALL);';

    EXECUTE 'lock table ' || table_name || ' in ACCESS EXCLUSIVE mode';

    EXECUTE 'alter table ' || secondary_table_name || ' rename to ' || history_table_name || ';';
    EXECUTE 'alter table ' || table_name || ' rename to ' || secondary_table_name || ';';
    EXECUTE 'alter table ' || new_table_name || ' rename to ' || table_name || ';';

    IF if_cold_ts_exists IS FALSE THEN
      EXECUTE 'drop table if exists ' || history_table_name || ';';
    END IF;

    RETURN 'Rotated, rows count: ' || rows_count;
  END;
  $BODY$
LANGUAGE plpgsql VOLATILE
COST 100;