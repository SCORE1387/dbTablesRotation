CREATE OR REPLACE PACKAGE ems_tables_rotation
IS
  PROCEDURE rotate_table(primary_table_name IN VARCHAR2);

  PROCEDURE move_history_logs_to_cold_ts(primary_table_name IN VARCHAR2);
END ems_tables_rotation;
/