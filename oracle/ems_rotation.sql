CREATE TABLE ems_rotation_log
(
  log_id numeric(20,0),
  operation varchar2(100),
  table_name varchar2(100),
  log_date timestamp,
  message varchar2(4000)
);

CREATE INDEX ems_rotation_log_id_idx ON ems_rotation_log (log_id);
CREATE INDEX ems_rotation_op_id_idx ON ems_rotation_log (operation);
CREATE INDEX ems_rotation_tn_id_idx ON ems_rotation_log (table_name);
CREATE INDEX ems_rotation_date_id_idx ON ems_rotation_log (log_date);

create sequence ems_rotation_seq
  increment by -1
  maxvalue 9999
  minvalue 1
  start with 9999
  nocache
  cycle;