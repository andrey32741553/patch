CREATE VIEW IF NOT EXISTS ${dds_schema_name}.servicec_analytic_vw_sstat as
	 SELECT
	        d.servicecall_id                                      as servicecall_ncode,
			d.analyticalfeat_id                                   as analyticalcat_ncode,
            nk.nk                                                 as analyticalcat_uk,
			d.dws_job                                             as dws_job,
			d.insert_date                                         as insert_date,
			CASE WHEN (dws_act = 'D') THEN 'Y' ELSE 'N' END       as deleted_flag,
			747                                                   as dws_emix,
			d.insert_date                                         as value_date,
			d.insert_date                                         as valid_from
	 FROM
	    ${ods_schema_name}.tbl_analyticalcat_nklink nk
	 JOIN
	    ${ods_schema_name}.tbl_servicec_analytic_delta d
	 ON d.analyticalfeat_id  = nk.id;