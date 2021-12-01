DROP TABLE IF EXISTS ${ods_schema_name}.tbl_objectba_analytic_src;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_objectba_analytic_src
(
    objectbase_id     bigint,
    analyticalfeat_id bigint,
    snapshot          int,
    deleted_flag      string,
    system_id         string,
    creation_date     string
)
    STORED AS ORC;