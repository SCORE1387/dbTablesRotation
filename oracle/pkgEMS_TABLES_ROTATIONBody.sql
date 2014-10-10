create or replace PACKAGE BODY ems_tables_rotation
IS

  PROCEDURE log_operation(
    log_id              IN NUMBER,
    table_name          IN VARCHAR2,
    operation           IN VARCHAR2,
    message             IN VARCHAR2
  )
  IS
  BEGIN
    INSERT INTO ems_rotation_log VALUES (log_id, operation, table_name, CURRENT_TIMESTAMP, message);
  END;

  PROCEDURE create_tbl_like_including_all(
    primary_table_name  IN VARCHAR2,
    new_table_name      IN VARCHAR2,
    new_idx_trg_postfix IN VARCHAR2 default '',
    new_idx_trg_prefix  IN VARCHAR2 default ''
  );

  PROCEDURE rotate_table(primary_table_name IN VARCHAR2)
  IS
    rows_count           NUMBER;
    secondary_rows_count NUMBER;
    history_table_num    NUMBER;
    secondary_table_name VARCHAR2(30);
    history_table_name   VARCHAR2(30);
    view_name            VARCHAR2(30);
    new_table_name       VARCHAR2(30);
    create_table_query   VARCHAR2(32000);
    ddl_query            VARCHAR2(32000);
    if_cold_ts_exists    NUMBER DEFAULT 0;
    rows_limit           NUMBER DEFAULT 5000000;
    errors               VARCHAR2(4000);
    proc_result          VARCHAR2(4000);
    --logging
    log_id               NUMBER;
    message              VARCHAR2(4000);
    operation            VARCHAR2(100);
  PRAGMA AUTONOMOUS_TRANSACTION; -- to avoid ORA-14552 executing ddl
    BEGIN
      select pkgutils.getid() into log_id from dual;
      operation := 'rotate_tables';

      log_operation(log_id, operation, primary_table_name, 'Start...');

      EXECUTE IMMEDIATE 'select count(*) from ' || primary_table_name INTO rows_count;
      SELECT COUNT(*) INTO if_cold_ts_exists FROM USER_TABLESPACES WHERE tablespace_name = 'EMS_HISTORICAL_DATA';

      log_operation(log_id, operation, primary_table_name, 'rows_count: ' || rows_count || '; if_cold_table_space_exists: ' || to_char(if_cold_ts_exists));

      BEGIN
        EXECUTE IMMEDIATE
        'SELECT value FROM ems_config WHERE key = ''ems.rotation.maxlogsize.' || primary_table_name || '''' INTO rows_limit;
        EXCEPTION
        WHEN INVALID_NUMBER THEN
          errors := 'Configuration error: ''ems.rotation.maxlogsize.' || primary_table_name ||
                  ''' key must be integer! ';
          log_operation(log_id, operation, primary_table_name, errors);
        WHEN NO_DATA_FOUND THEN
          log_operation(log_id, operation, primary_table_name, '''ems.rotation.maxlogsize.' || primary_table_name ||
                  ''' key not specified.');
      END;

      log_operation(log_id, operation, primary_table_name, 'rows_limit: ' || rows_limit);

      IF rows_count < rows_limit
      THEN
        proc_result := 'Not rotated, rows count = ' || coalesce(rows_count, '0');
        log_operation(log_id, operation, primary_table_name, proc_result);
        COMMIT;
        RETURN;
      END IF;

      secondary_table_name := primary_table_name || '_2';
      new_table_name := 'NEW_' || primary_table_name;
      view_name := primary_table_name || '_VIEW';

      SELECT EMS_ROTATION_SEQ.NEXTVAL INTO history_table_num FROM dual;
      history_table_name := 'H$' || primary_table_name || '_' || history_table_num;

      log_operation(log_id, operation, primary_table_name, 'secondary_table_name: ' || secondary_table_name || '; new_table_name: ' || new_table_name || '; history_table_name: ' || history_table_name);

      EXECUTE IMMEDIATE 'select count(*) from ' || secondary_table_name INTO secondary_rows_count;

      log_operation(log_id, operation, primary_table_name, 'secondary_rows_count: ' || secondary_rows_count);

      log_operation(log_id, operation, primary_table_name, 'Creating new table...');
      create_tbl_like_including_all(primary_table_name, new_table_name, '', 'NEW_');
      log_operation(log_id, operation, primary_table_name, 'New table created');

      log_operation(log_id, operation, primary_table_name, 'Renaming tables...');
      BEGIN
        EXECUTE IMMEDIATE 'lock table ' || primary_table_name || ' IN EXCLUSIVE MODE';

        EXECUTE IMMEDIATE 'alter table ' || secondary_table_name || ' rename to ' || history_table_name;
        EXECUTE IMMEDIATE 'alter table ' || primary_table_name || ' rename to ' || secondary_table_name;
        EXECUTE IMMEDIATE 'alter table ' || new_table_name || ' rename to ' || primary_table_name;
        COMMIT;

      END;
      log_operation(log_id, operation, primary_table_name, 'Tables renamed');

      --rename indexes and triggers for history table
      log_operation(log_id, operation, primary_table_name, 'Renaming indexes and triggers for history table...');
      FOR trg IN (SELECT trgs.trigger_name FROM user_triggers trgs WHERE trgs.table_name = history_table_name)
      LOOP
        log_operation(log_id, operation, primary_table_name, 'Renaming trigger: ' || trg.trigger_name);
        EXECUTE IMMEDIATE 'alter trigger ' || trg.trigger_name || ' rename to ' ||
                          'H$' ||
                          substr(trg.trigger_name, 1, length(trg.trigger_name)-2) ||
                          '_' || to_char(history_table_num);
      END LOOP;
      FOR idx IN (SELECT idxs.index_name FROM user_indexes idxs WHERE idxs.table_name = history_table_name)
      LOOP
        log_operation(log_id, operation, primary_table_name, 'Renaming index: ' || idx.index_name);
        EXECUTE IMMEDIATE 'alter index ' || idx.index_name || ' rename to ' ||
                          'H$' ||
                          substr(idx.index_name, 1, length(idx.index_name)-2) ||
                          '_' || to_char(history_table_num);
      END LOOP;

      log_operation(log_id, operation, primary_table_name, 'Indexes and triggers for history table renamed');

      --rename indexes and triggers for secondary table
      log_operation(log_id, operation, primary_table_name, 'Renaming indexes and triggers for secondary table...');
      FOR trg IN (SELECT trgs.trigger_name FROM user_triggers trgs WHERE trgs.table_name = secondary_table_name)
      LOOP
        log_operation(log_id, operation, primary_table_name, 'Renaming trigger: ' || trg.trigger_name);
        EXECUTE IMMEDIATE 'alter trigger ' || trg.trigger_name || ' rename to ' || trg.trigger_name || '_2';
      END LOOP;
      FOR idx IN (SELECT idxs.index_name FROM user_indexes idxs WHERE idxs.table_name = secondary_table_name)
      LOOP
        log_operation(log_id, operation, primary_table_name, 'Renaming index: ' || idx.index_name);
        EXECUTE IMMEDIATE 'alter index ' || idx.index_name || ' rename to ' || idx.index_name || '_2';
      END LOOP;

      log_operation(log_id, operation, primary_table_name, 'Indexes and triggers for secondary table renamed');

      --rename indexes and triggers for primary table
      log_operation(log_id, operation, primary_table_name, 'Renaming indexes and triggers for primary table...');
      FOR trg IN (SELECT trgs.trigger_name FROM user_triggers trgs WHERE trgs.table_name = primary_table_name)
      LOOP
        log_operation(log_id, operation, primary_table_name, 'Renaming trigger: ' || trg.trigger_name);
        EXECUTE IMMEDIATE 'alter trigger ' || trg.trigger_name || ' rename to ' || substr(trg.trigger_name, 5);
      END LOOP;
      FOR idx IN (SELECT idxs.index_name FROM user_indexes idxs WHERE idxs.table_name = primary_table_name)
      LOOP
        log_operation(log_id, operation, primary_table_name, 'Renaming index: ' || idx.index_name);
        EXECUTE IMMEDIATE 'alter index ' || idx.index_name || ' rename to ' || substr(idx.index_name, 5);
      END LOOP;

      log_operation(log_id, operation, primary_table_name, 'Indexes and triggers for primary table renamed');

      IF if_cold_ts_exists = 0 OR secondary_rows_count = 0
      THEN
        --drop table with all constraints
        log_operation(log_id, operation, primary_table_name, 'Dropping history table...');
        EXECUTE IMMEDIATE 'drop table ' || history_table_name || ' cascade CONSTRAINTS';
        log_operation(log_id, operation, primary_table_name, 'History table dropped');
      END IF;

      --rebuild view
      log_operation(log_id, operation, primary_table_name, 'Rebuilding view...');
      execute immediate 'select * from ' || view_name || ' where 1=0';

      log_operation(log_id, operation, primary_table_name, 'Finish...');

      proc_result := 'Result: rotated, rows count: ' || coalesce(rows_count, '0') || '. Warnings: ' || coalesce(errors, 'none');
      log_operation(log_id, operation, primary_table_name, proc_result);
      COMMIT;
    END;

  PROCEDURE move_history_logs_to_cold_ts(primary_table_name IN VARCHAR2)
  IS
    history_table_pattern      VARCHAR2(100);
    history_tables             arrayofstrings := arrayofstrings();
    history_tables_in_cold_ts  arrayofstrings := arrayofstrings();
    indexes_on_history_tables  arrayofstrings := arrayofstrings();
    triggers_on_history_tables arrayofstrings := arrayofstrings();
    if_cold_ts_exists          NUMBER default 0;
    history_table              VARCHAR2(100);
    index_name                 VARCHAR2(100);
    history_tables_max_num     NUMBER DEFAULT 2000;
    num_of_dropped_tables      NUMBER;
    errors                     VARCHAR2(4000);
    proc_result                VARCHAR2(4000);
    --logging
    log_id                     NUMBER;
    message                    VARCHAR2(4000);
    operation                  VARCHAR2(100);
  PRAGMA AUTONOMOUS_TRANSACTION; -- to avoid ORA-14552 executing ddl
    BEGIN
      select pkgutils.getid() into log_id from dual;
      operation := 'move_history_logs_to_cold_ts';

      --move history tables to cold tablespace start
      log_operation(log_id, operation, primary_table_name, 'Start moving history tables to cold tablespace...');
      SELECT COUNT(*) INTO if_cold_ts_exists FROM USER_TABLESPACES WHERE tablespace_name = 'EMS_HISTORICAL_DATA';
      IF if_cold_ts_exists = 0 THEN
        proc_result := 'History tables were not moved. Cold tablespace ''ems_historical_data'' does not exist!';
        log_operation(log_id, operation, primary_table_name, proc_result);
        COMMIT;
        RETURN;
      END IF;
      log_operation(log_id, operation, primary_table_name, 'if_cold_ts_exists: ' || if_cold_ts_exists);

      history_table_pattern := 'H$' || primary_table_name || '_%';
      log_operation(log_id, operation, primary_table_name, 'history_table_pattern: ' || history_table_pattern);

      EXECUTE IMMEDIATE
      'select
        table_name
      from
        user_tables
      where
        table_name like ''' || history_table_pattern || '''
        and (tablespace_name != ''EMS_HISTORICAL_DATA'' or tablespace_name is null)'
      BULK COLLECT INTO history_tables;

      FOR i IN 1 .. history_tables.COUNT LOOP
        log_operation(log_id, operation, primary_table_name, 'Moving history_table: ' || history_tables(i));
        EXECUTE IMMEDIATE 'ALTER TABLE ' || history_tables(i) || ' MOVE TABLESPACE ems_historical_data';
      END LOOP;

      --drop all indexes and triggers
      log_operation(log_id, operation, primary_table_name, 'Dropping all indexes and triggers...');
      EXECUTE IMMEDIATE
      'select index_name from user_indexes where table_name in (select column_value from table (:history_tables))'
      BULK COLLECT INTO indexes_on_history_tables
      USING history_tables;

      EXECUTE IMMEDIATE
      'select trigger_name from user_triggers where table_name in (select column_value from table (:history_tables))'
      BULK COLLECT INTO triggers_on_history_tables
      USING history_tables;

      FOR i IN 1 .. indexes_on_history_tables.COUNT LOOP
        log_operation(log_id, operation, primary_table_name, 'index on history_table: ' || indexes_on_history_tables(i));
        EXECUTE IMMEDIATE 'DROP INDEX ' || indexes_on_history_tables(i);
      END LOOP;

      FOR i IN 1 .. triggers_on_history_tables.COUNT LOOP
        log_operation(log_id, operation, primary_table_name, 'Trigger on history_table: ' || triggers_on_history_tables(i));
        EXECUTE IMMEDIATE 'DROP TRIGGER ' || triggers_on_history_tables(i);
      END LOOP;
      log_operation(log_id, operation, primary_table_name, 'All indexes and triggers dropped');

      log_operation(log_id, operation, primary_table_name, 'Move history tables to cold tablespace end');
      --move history tables to cold tablespace end

      --remove too old history tables start
      log_operation(log_id, operation, primary_table_name, 'Removing too old history tables...');
      BEGIN
        SELECT value INTO history_tables_max_num FROM ems_config WHERE key = 'ems.rotation.historytables.maxnum';
        EXCEPTION
        WHEN INVALID_NUMBER THEN
          errors := 'Configuration error: ''ems.rotation.historytables.maxnum'' key must be integer! ';
          log_operation(log_id, operation, primary_table_name, errors);
        WHEN NO_DATA_FOUND THEN
          log_operation(log_id, operation, primary_table_name, '''ems.rotation.historytables.maxnum'' key not specified ');
      END;

      log_operation(log_id, operation, primary_table_name, 'history_tables_max_num: ' || history_tables_max_num);

      EXECUTE IMMEDIATE
      'select ut.table_name
       from
         user_tables ut,
         user_objects uo
       where
         ut.table_name like :history_table_pattern
         and ut.tablespace_name = ''EMS_HISTORICAL_DATA''
         and uo.object_name = ut.table_name
       order by uo.created'
      BULK COLLECT INTO history_tables_in_cold_ts
      USING history_table_pattern;

      IF history_tables_in_cold_ts.COUNT > history_tables_max_num THEN
        FOR i IN 1 ..  history_tables_in_cold_ts.COUNT - history_tables_max_num
        LOOP
          log_operation(log_id, operation, primary_table_name, 'history_table in cold tablespace: ' || history_tables_in_cold_ts(i));
          EXECUTE IMMEDIATE 'DROP TABLE ' || history_tables_in_cold_ts(i) || ' CASCADE CONSTRAINTS';
        END LOOP;
        num_of_dropped_tables := history_tables_in_cold_ts.COUNT - history_tables_max_num;
      END IF;
      log_operation(log_id, operation, primary_table_name, 'Too old history tables removed');
      --remove too old history tables end

      proc_result := coalesce(history_tables.COUNT, '0') || ' history tables were moved to cold tablespace. ' ||
             coalesce(num_of_dropped_tables, '0') || ' old tables were dropped from cold tablespace.'  ||
             '. Warnings: ' || coalesce(errors, 'none');
      log_operation(log_id, operation, primary_table_name, proc_result);
      COMMIT;
    END;

  PROCEDURE create_tbl_like_including_all(
    primary_table_name  IN VARCHAR2,
    new_table_name      IN VARCHAR2,
    new_idx_trg_postfix IN VARCHAR2 default '',
    new_idx_trg_prefix  IN VARCHAR2 default ''
  )
  IS
    ddl_query           VARCHAR2(32000);
    BEGIN
      --DBMS_OUTPUT.PUT_LINE('create_tbl_like_including_all start: ' || primary_table_name || ', ' || new_table_name);
      --create new table
      SELECT
        replace(dbms_metadata.get_ddl('TABLE', primary_table_name), primary_table_name, new_table_name)
      INTO ddl_query
      FROM dual;
      ddl_query := substr(ddl_query, 1, length(ddl_query) - 1);
      --DBMS_OUTPUT.PUT_LINE('Query: ' || ddl_query);
      EXECUTE IMMEDIATE ddl_query;

      --DBMS_OUTPUT.PUT_LINE('Table created');

      --create triggers for new table
      FOR trg IN (SELECT trgs.trigger_name FROM user_triggers trgs WHERE trgs.table_name = primary_table_name)
      LOOP
        ddl_query := REPLACE(
            REPLACE(dbms_metadata.get_ddl('TRIGGER', trg.trigger_name), primary_table_name, new_table_name),
            trg.trigger_name, new_idx_trg_prefix || trg.trigger_name || new_idx_trg_postfix);
        ddl_query := substr(ddl_query, 1, length(ddl_query) - 1);
        EXECUTE IMMEDIATE ddl_query;
      END LOOP;

      --DBMS_OUTPUT.PUT_LINE('Triggers created');

      --create indexes for new table
      FOR idx IN (SELECT idxs.index_name FROM user_indexes idxs WHERE idxs.table_name = primary_table_name)
      LOOP
        ddl_query := REPLACE(
            REPLACE(dbms_metadata.get_ddl('INDEX', idx.index_name), primary_table_name, new_table_name),
            idx.index_name, new_idx_trg_prefix || idx.index_name || new_idx_trg_postfix);
        ddl_query := substr(ddl_query, 1, length(ddl_query) - 1);
        EXECUTE IMMEDIATE ddl_query;
      END LOOP;

      --DBMS_OUTPUT.PUT_LINE('Indexes created');

    END;

END ems_tables_rotation;
/