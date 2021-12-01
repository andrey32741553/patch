DROP TABLE IF EXISTS ${ods_schema_name}.tbl_objectba_analytic;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_objectba_analytic
(
    dws_job           string,
    insert_date       string,
    deleted_flag      string,
    objectbase_id     bigint,
    analyticalfeat_id bigint
)
    STORED AS ORC;