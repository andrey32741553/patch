DROP TABLE IF EXISTS ${ods_schema_name}.tbl_servicec_configit_src;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_servicec_configit_src
(
    servicecall_id bigint,
    configitems_id bigint,
    snapshot       int,
    deleted_flag   string,
    system_id      string,
    creation_date  string
)
    STORED AS ORC;