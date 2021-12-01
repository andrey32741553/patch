CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_analyticalcat_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_analyticalcat;