FROM (SELECT *, 
                      count(objectbase_id) over (partition by objectbase_id, analyticalfeat_id order by insert_date desc) as count_num,
                      row_number() over (partition by objectbase_id, analyticalfeat_id order by insert_date desc) as row_num FROM
                (SELECT
                    dws_job, insert_date, dws_act,
                    objectbase_id, analyticalfeat_id
                FROM (SELECT '${dws_job}'                                AS dws_job,
                             '${insert_date}'                              AS insert_date,
                             CASE
                                 WHEN (((mirror.objectbase_id is null) or (mirror.analyticalfeat_id is null))
                                        or (mirror.objectbase_id = src.objectbase_id and mirror.analyticalfeat_id = src.analyticalfeat_id and '${insert_date}' > mirror.insert_date))
                                     THEN 'I'
                                 WHEN (src.objectbase_id is null) or (src.analyticalfeat_id is null)
                                     THEN 'D'
                                 ELSE ''
                             END                                 AS dws_act,
                             
                     CASE
                         WHEN (src.objectbase_id is not null)
                             THEN src.objectbase_id
                         WHEN (mirror.objectbase_id is not null)
                             THEN mirror.objectbase_id
                         ELSE null
                     END AS objectbase_id
        , 
                     CASE
                         WHEN (src.objectbase_id is not null)
                             THEN src.analyticalfeat_id
                         WHEN (mirror.objectbase_id is not null)
                             THEN mirror.analyticalfeat_id
                         ELSE null
                     END AS analyticalfeat_id
                    
                      FROM ${ods_schema_name}.tbl_objectba_analytic_src AS src
                      FULL OUTER JOIN ${ods_schema_name}.tbl_objectba_analytic
                      AS mirror ON (src.objectbase_id = mirror.objectbase_id) and (src.analyticalfeat_id = mirror.analyticalfeat_id)) delta WHERE delta.dws_act <> "") delta_num ) rsl

                INSERT INTO TABLE ${ods_schema_name}.tbl_objectba_analytic_delta partition (`date`)
                    SELECT dws_job, dws_act,
                    objectbase_id, analyticalfeat_id,
                    insert_date, '${date}' as `date`
                WHERE row_num = 1

                INSERT INTO TABLE ${ods_schema_name}.tbl_objectba_analytic_double
                    SELECT dws_job,
                    insert_date,
                    CASE 
                        WHEN (row_num = 1) THEN 'Y'
                    ELSE 'N' END AS effective_flag,
                    1 as algorithm_ncode,
                    objectbase_id, analyticalfeat_id
                    WHERE count_num > 1;