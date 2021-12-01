FROM (SELECT *, 
                      count(servicecall_id) over (partition by servicecall_id, configitems_id order by insert_date desc) as count_num,
                      row_number() over (partition by servicecall_id, configitems_id order by insert_date desc) as row_num FROM
                (SELECT
                    dws_job, insert_date, dws_act,
                    servicecall_id, configitems_id
                FROM (SELECT '${dws_job}'                                AS dws_job,
                             '${insert_date}'                            AS insert_date,
                             CASE
                                 WHEN (((mirror.servicecall_id is null) or (mirror.configitems_id is null))
                                         or (mirror.servicecall_id = src.servicecall_id and mirror.configitems_id = src.configitems_id and '${insert_date}' > mirror.insert_date))
                                     THEN 'I'
                                 WHEN (src.servicecall_id is null) or (src.configitems_id is null)
                                     THEN 'D'
                                 ELSE ''
                             END                                 AS dws_act,
                             
                     CASE
                         WHEN (src.servicecall_id is not null)
                             THEN src.servicecall_id
                         WHEN (mirror.servicecall_id is not null)
                             THEN mirror.servicecall_id
                         ELSE null
                     END AS servicecall_id
        , 
                     CASE
                         WHEN (src.servicecall_id is not null)
                             THEN src.configitems_id
                         WHEN (mirror.servicecall_id is not null)
                             THEN mirror.configitems_id
                         ELSE null
                     END AS configitems_id

                      FROM ${ods_schema_name}.tbl_servicec_configit_src AS src
                      FULL OUTER JOIN ${ods_schema_name}.tbl_servicec_configit
                      AS mirror ON (src.servicecall_id = mirror.servicecall_id) and (src.configitems_id = mirror.configitems_id)) delta WHERE delta.dws_act <> "") delta_num ) rsl

                INSERT INTO TABLE ${ods_schema_name}.tbl_servicec_configit_delta partition (`date`)
                    SELECT dws_job, dws_act,
                    servicecall_id, configitems_id,
                    insert_date, '${date}' as `date`
                WHERE row_num = 1

                INSERT INTO TABLE ${ods_schema_name}.tbl_servicec_configit_double
                    SELECT dws_job,
                    insert_date,
                    CASE 
                        WHEN (row_num = 1) THEN 'Y'
                    ELSE 'N' END AS effective_flag,
                    1 as algorithm_ncode,
                    servicecall_id, configitems_id
                    WHERE count_num > 1;