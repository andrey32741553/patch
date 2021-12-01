CREATE VIEW IF NOT EXISTS ${dds_schema_name}.objectba_analytic_vw_sstat as
	 SELECT
	        d.objectbase_id                                       as objectbase_ncode,
			d.analyticalfeat_id                                   as analyticalcat_ncode,
            onk.nk                                                as objectbase_uk,
            ank.nk                                                as analyticalcat_uk,
			d.dws_job                                             as dws_job,
			d.insert_date                                         as insert_date,
			CASE WHEN (dws_act = 'D') THEN 'Y' ELSE 'N' END       as deleted_flag,
			748                                                   as dws_emix,
			d.insert_date                                         as value_date,
			d.insert_date                                         as valid_from
     FROM
	    ${ods_schema_name}.tbl_objectba_analytic_delta d
	 JOIN
	    ${ods_schema_name}.tbl_objectbase_nklink onk
	 ON d.objectbase_id = onk.id
	 JOIN
	    ${ods_schema_name}.tbl_analyticalcat_nklink ank
	 ON d.analyticalfeat_id = ank.id;