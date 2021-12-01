DROP TABLE IF EXISTS ${ods_schema_name}.tbl_servicec_configit;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_servicec_configit
(
    dws_job        string,
    insert_date    string,
    deleted_flag   string,
    servicecall_id bigint,
    configitems_id bigint
)
    STORED AS ORC;