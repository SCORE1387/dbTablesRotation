BEGIN
  DBMS_SCHEDULER.CREATE_JOB(job_name      => '"ROTATE_TABLES_JOB"',
                          job_type        => 'PLSQL_BLOCK',
                          JOB_ACTION      => 'BEGIN' ||
                                               ' ems_tables_rotation.rotate_table(''TABLE_FOR_ROTATION'');' ||
                                             'END;',
                          start_date      => SYSTIMESTAMP,
                          repeat_interval => 'freq=hourly',
                          end_date        => NULL,
                          enabled         => TRUE,
                          comments        => 'Rotate tables ''TABLE_FOR_ROTATION''');
  DBMS_SCHEDULER.set_attribute( name => '"ROTATE_TABLES_JOB"', attribute => 'logging_level', value => DBMS_SCHEDULER.LOGGING_FULL);

  DBMS_SCHEDULER.CREATE_JOB(job_name      => '"MOVE_HISTORY_TABLES_TO_COLD_TS"',
                          job_type        => 'PLSQL_BLOCK',
                          JOB_ACTION      => 'BEGIN' ||
                                                ' ems_tables_rotation.move_history_logs_to_cold_ts(''TABLE_FOR_ROTATION'');' ||
                                              'END;',
                          start_date      => SYSTIMESTAMP,
                          repeat_interval => 'freq=daily',
                          end_date        => NULL,
                          enabled         => TRUE,
                          comments        => 'Move history tables to ''EMS_HISTORICAL_DATA'' tablespace for ''TABLE_FOR_ROTATION''');
  DBMS_SCHEDULER.set_attribute( name => '"MOVE_HISTORY_TABLES_TO_COLD_TS"', attribute => 'logging_level', value => DBMS_SCHEDULER.LOGGING_FULL);
END;
/