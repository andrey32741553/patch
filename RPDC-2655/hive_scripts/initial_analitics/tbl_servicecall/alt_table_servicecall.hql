ALTER TABLE ${ods_schema_name}.tbl_servicecall ADD COLUMNS (overduestate_s  string,
                                                            overduestate_d  bigint);

ALTER TABLE ${ods_schema_name}.tbl_servicecall_src ADD COLUMNS (overduestate_s  string,
                                                                overduestate_d  bigint);

ALTER TABLE ${ods_schema_name}.tbl_servicecall_delta ADD COLUMNS (overduestate_s  string,
                                                                  overduestate_d  bigint);

ALTER TABLE ${ods_schema_name}.tbl_servicecall_double ADD COLUMNS (overduestate_s  string,
                                                                   overduestate_d  bigint);