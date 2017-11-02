--CREATE SCHEMA
CREATE SCHEMA IF NOT EXISTS sch_chameleon;

--VIEWS
CREATE OR REPLACE VIEW sch_chameleon.v_version 
 AS
	SELECT '1.6'::TEXT t_version
;

--TABLES/INDICES	
CREATE TABLE sch_chameleon.t_sources
(
	i_id_source	INT8 IDENTITY(1,1),
	t_source		text NOT NULL,
	t_dest_schema   text NOT NULL,
	enm_status character varying(255) NOT NULL DEFAULT 'ready',
	ts_last_received timestamp without time zone,
	ts_last_replay timestamp without time zone,
	v_log_table character varying(1000) ,
	CONSTRAINT pk_t_sources PRIMARY KEY (i_id_source)
);

CREATE TABLE sch_chameleon.t_replica_batch
(
  i_id_batch INT8 NOT NULL IDENTITY(1,1),
  i_id_source bigint NOT NULL,
  t_binlog_name text,
  i_binlog_position integer,
  b_started boolean NOT NULL DEFAULT False,
  b_processed boolean NOT NULL DEFAULT False,
  b_replayed boolean NOT NULL DEFAULT False,
  ts_created timestamp without time zone NOT NULL DEFAULT GETDATE(),
  ts_processed timestamp without time zone ,
  ts_replayed timestamp without time zone ,
  i_replayed bigint NULL,
  i_skipped bigint NULL,
  i_ddl bigint NULL,
  CONSTRAINT pk_t_batch PRIMARY KEY (i_id_batch)
);

CREATE TABLE IF NOT EXISTS sch_chameleon.t_log_replica
(
  i_id_event INT8 NOT NULL IDENTITY(1,1),
  i_id_batch INT8 NOT NULL,
  v_table_name character varying(100) NOT NULL,
  v_schema_name character varying(100) NOT NULL,
  enm_binlog_event character varying(255) NOT NULL,
  t_binlog_name text,
  i_binlog_position integer,
  ts_event_datetime timestamp without time zone NOT NULL DEFAULT GETDATE(),
  jsb_event_data text,
  jsb_event_update text,
  t_query text,
  i_my_event_time bigint,
  CONSTRAINT pk_log_replica PRIMARY KEY (i_id_event),
  CONSTRAINT fk_replica_batch FOREIGN KEY (i_id_batch) 
	REFERENCES  sch_chameleon.t_replica_batch (i_id_batch)
);

CREATE TABLE sch_chameleon.t_replica_tables
(
  i_id_table INT8 NOT NULL IDENTITY(1,1),
  i_id_source bigint NOT NULL,
  v_table_name character varying(100) NOT NULL,
  v_schema_name character varying(100) NOT NULL,
  v_table_pkey character varying(100) NOT NULL,
  t_binlog_name text,
  i_binlog_position integer,
  CONSTRAINT pk_t_replica_tables PRIMARY KEY (i_id_table)
);

CREATE TABLE sch_chameleon.t_discarded_rows
(
	i_id_row		INT8 NOT NULL IDENTITY(1,1),
	i_id_batch	bigint NOT NULL,
	ts_discard	timestamp with time zone NOT NULL DEFAULT GETDATE(),
	t_row_data	text,
	CONSTRAINT pk_t_discarded_rows PRIMARY KEY (i_id_row)
);
	
ALTER TABLE sch_chameleon.t_replica_batch
	ADD CONSTRAINT fk_t_replica_batch_i_id_source FOREIGN KEY (i_id_source)
	REFERENCES sch_chameleon.t_sources (i_id_source);

ALTER TABLE sch_chameleon.t_replica_tables
	ADD CONSTRAINT fk_t_replica_tables_i_id_source FOREIGN KEY (i_id_source)
	REFERENCES sch_chameleon.t_sources (i_id_source);


CREATE TABLE sch_chameleon.t_index_def
(
  i_id_def INT8 NOT NULL IDENTITY(1,1),
  i_id_source bigint NOT NULL,
  v_schema character varying(100),
  v_table character varying(100),
  v_index character varying(100),
  t_create	text,
  t_drop	text,
  CONSTRAINT pk_t_index_def PRIMARY KEY (i_id_def)
);

CREATE TABLE sch_chameleon.t_batch_events
(
	i_id_batch	bigint NOT NULL IDENTITY(1,1),
	I_id_event	bigint NOT NULL,
	CONSTRAINT pk_t_batch_id_events PRIMARY KEY (i_id_batch)
);

ALTER TABLE sch_chameleon.t_batch_events
	ADD CONSTRAINT fk_t_batch_id_events_i_id_batch FOREIGN KEY (i_id_batch)
	REFERENCES sch_chameleon.t_replica_batch(i_id_batch);
