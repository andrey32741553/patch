INSERT OVERWRITE TABLE ${ods_schema_name}.tbl_servicec_analytic
SELECT dws_job,
       insert_date,
       deleted_flag,
       servicecall_id,
       analyticalfeat_id
FROM (SELECT *, row_number() over (partition by servicecall_id order by insert_date desc) as row_num
      FROM (
               SELECT dws_job,
                      deleted_flag,
                      insert_date,
                      servicecall_id,
                      analyticalfeat_id
               FROM ${ods_schema_name}.tbl_servicec_analytic
               UNION ALL
               SELECT '${dws_job}'                                    as dws_job,
                      CASE WHEN (dws_act = 'D') THEN 'Y' ELSE 'N' END as deleted_flag,
                      '${insert_date}'                                as insert_date,
                      servicecall_id,
                      analyticalfeat_id
               FROM ${ods_schema_name}.tbl_servicec_analytic_delta
               WHERE date = '${insert_date_delta}'
           ) union_delta_mirror) mirror
where row_num = 1