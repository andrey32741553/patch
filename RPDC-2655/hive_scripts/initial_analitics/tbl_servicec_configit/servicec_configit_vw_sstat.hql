CREATE VIEW IF NOT EXISTS ${dds_schema_name}.servicec_configit_vw_sstat as
	 SELECT
	        d.servicecall_id                                      as servicecall_ncode,
			d.configitems_id                                      as objectbase_ncode,
			nk.nk                                                 as objectbase_uk,
			d.dws_job                                             as dws_job,
			d.insert_date                                         as insert_date,
			CASE WHEN (dws_act = 'D') THEN 'Y' ELSE 'N' END       as deleted_flag,
			746                                                   as dws_emix,
			d.insert_date                                         as value_date,
			d.insert_date                                         as valid_from
	 FROM
	    ${ods_schema_name}.tbl_objectbase_nklink nk
	 JOIN
	    ${ods_schema_name}.tbl_servicec_configit_delta d
	 ON d.configitems_id = nk.id;