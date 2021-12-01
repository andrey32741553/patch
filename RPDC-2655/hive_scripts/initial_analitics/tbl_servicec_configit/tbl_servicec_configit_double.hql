DROP TABLE IF EXISTS ${ods_schema_name}.tbl_servicec_configit_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_servicec_configit_double
(
    dws_job         string,
    insert_date     string,
    effective_flag  string,
    algorithm_ncode int,
    servicecall_id  bigint,
    configitems_id  bigint
)
    STORED AS ORC;