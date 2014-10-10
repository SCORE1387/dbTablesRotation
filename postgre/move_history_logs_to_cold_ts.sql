CREATE OR REPLACE FUNCTION move_history_logs_to_cold_ts(table_name TEXT)
  RETURNS TEXT AS
  $BODY$
  DECLARE
    secondary_table_name      TEXT;
    history_table_pattern     TEXT;
    history_tables            TEXT [];
    indexes_on_history_tables TEXT [];
    if_cold_ts_exists         BOOLEAN;
    history_table             TEXT;
    index_name                TEXT;
  BEGIN
    SELECT EXISTS(SELECT * FROM pg_tablespace WHERE spcname = 'ems_historical_data') INTO if_cold_ts_exists;
    IF if_cold_ts_exists IS FALSE THEN RETURN 'History tables were not moved. Cold tablespace ''ems_historical_data'' does not exist!'; END IF;

    history_table_pattern := table_name || '_%';
    secondary_table_name := table_name || '_2';

    EXECUTE
    'select
      array_agg(table_name::text)
    from
      information_schema.tables
    where
      table_name like ''' || history_table_pattern || '''
      and table_name != ''' || secondary_table_name || '''
      and table_type = ''BASE TABLE''
      and table_schema != ''ems_historical_data'';'
    INTO history_tables;

    SELECT
      array_agg(i.relname :: TEXT)
    FROM pg_class t, pg_class i, pg_index ix
    WHERE
      t.oid = ix.indrelid
      AND i.oid = ix.indexrelid
      AND t.relname IN (SELECT * FROM unnest(history_tables))
    INTO indexes_on_history_tables;

    FOREACH index_name IN ARRAY indexes_on_history_tables LOOP
      EXECUTE 'DROP INDEX' || index_name || ';';
    END LOOP;

    FOREACH history_table IN ARRAY history_tables LOOP
      EXECUTE 'ALTER TABLE' || history_table || 'SET TABLESPACE ''ems_historical_data'';';
    END LOOP;
    RETURN 'History tables moved to cold tablespace: ' || array_to_string(history_tables, ', ') || '. Indexes: ' ||
           array_to_string(indexes_on_history_tables, ', ');
  END;
  $BODY$
LANGUAGE plpgsql VOLATILE
COST 100;