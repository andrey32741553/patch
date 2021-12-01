DROP TABLE IF EXISTS ${ods_schema_name}.tbl_servicec_analytic_delta;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_servicec_analytic_delta
(
    dws_job           string,
    dws_act           string,
    servicecall_id    bigint,
    analyticalfeat_id bigint,
    insert_date       string
) PARTITIONED BY (`date` string)
    STORED AS ORC;