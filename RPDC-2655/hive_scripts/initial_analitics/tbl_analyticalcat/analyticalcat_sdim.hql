CREATE VIEW IF NOT EXISTS ${dds_schema_name}.analyticalcat_sdim as
SELECT
                   d.nk                                                                as uk,
                   d.id                                                                as ncode,
                   from_utc_timestamp(d.creation_date, 'Europe/Moscow')                as creation_msk_dts,
                   from_utc_timestamp(d.last_modified_date, 'Europe/Moscow')           as last_modified_msk_dts,
                   from_utc_timestamp(d.removal_date, 'Europe/Moscow')                 as removal_msk_dts,
                   CASE WHEN (d.removed = 1) THEN 'Y' ELSE 'N' END                     as removed_flag,
                   d.title                                                             as name,
                   d.case_id                                                           as type_ccode,
                   d.num_                                                              as num_ncode,
                   from_utc_timestamp(d.responsiblestarttime, 'Europe/Moscow')         as responsiblestarttime_msk_dts,
                   d.state                                                             as state_ccode,
                   from_utc_timestamp(d.statestarttime, 'Europe/Moscow')               as statestarttime_msk_dts,
                   ank1.nk                                                             as parent_uk,
                   enk1.nk                                                             as employee_author_uk,
                   enk2.nk                                                             as employee_responsible_uk,
                   tnk.nk                                                              as team_responsible_uk,
                   ank2.nk                                                             as parent_head_uk,
                   CASE WHEN (catalog_needcheck = 1) THEN 'Y' ELSE 'N' END             as needcheck_flag,
                   CASE WHEN (catalog_needcopy = 1) THEN 'Y' ELSE 'N' END              as needcopy_flag,
                   d.dws_job                                                           as dws_job,
			       d.insert_date                                                       as insert_date,
			       CASE WHEN (d.dws_act = 'D') THEN 'Y' ELSE 'N' END                   as deleted_flag,
			       749                                                                 as dws_emix,
                   d.insert_date                                                       as valid_from,
                   "N"                                                                 as default_flag
FROM ${ods_schema_name}.tbl_analyticalcat_delta d
LEFT JOIN ${ods_schema_name}.tbl_analyticalcat_nklink ank1 ON d.parent = ank1.id
LEFT JOIN ${ods_schema_name}.tbl_employee_nklink enk1 ON d.author_id = enk1.id
LEFT JOIN ${ods_schema_name}.tbl_employee_nklink enk2 ON d.responsibleemployee_id = enk2.id
LEFT JOIN ${ods_schema_name}.tbl_team_nklink tnk ON d.responsibleteam_id = tnk.id
LEFT JOIN ${ods_schema_name}.tbl_analyticalcat_nklink ank2 ON d.headcat  = ank2.id;
