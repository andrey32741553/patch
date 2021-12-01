DROP TABLE IF EXISTS ${ods_schema_name}.tbl_servicec_analytic;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_servicec_analytic
(
    dws_job           string,
    insert_date       string,
    deleted_flag      string,
    servicecall_id    bigint,
    analyticalfeat_id bigint
)
    STORED AS ORC;