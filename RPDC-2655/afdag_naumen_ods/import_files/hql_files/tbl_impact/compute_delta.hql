FROM (SELECT *, 
                      count(id) over (partition by id order by insert_date desc) as count_num,
                      row_number() over (partition by id order by insert_date desc) as row_num FROM
                (SELECT 
                    dws_job, insert_date, dws_act, nk, dws_uniact,
                    id, removal_date, removed, code, color, folder, pos, parent, title_en, title_client, title
                FROM (SELECT coalesce(mirror.nk, reflect("java.util.UUID", "randomUUID")) AS nk,
                             '${dws_job}'                                AS dws_job,
                             '${insert_date}'                              AS insert_date,
                             'N'                                     AS dws_uniact,
                             CASE
                                 WHEN ((mirror.id is null) or (mirror.id = src.id and mirror.deleted_flag = 'Y' and '${insert_date}' > mirror.insert_date))
                                     THEN 'I'
                                 WHEN (src.id is null and mirror.deleted_flag = 'N')
                                     THEN 'D'
                                 WHEN (mirror.deleted_flag = 'N') and ((src.removal_date <=> mirror.removal_date) = FALSE or (src.removed <=> mirror.removed) = FALSE or (src.code <=> mirror.code) = FALSE or (src.color <=> mirror.color) = FALSE or (src.folder <=> mirror.folder) = FALSE or (src.pos <=> mirror.pos) = FALSE or (src.parent <=> mirror.parent) = FALSE or (src.title_en <=> mirror.title_en) = FALSE or (src.title_client <=> mirror.title_client) = FALSE or (src.title <=> mirror.title) = FALSE) 
                                     THEN 'U'
                                 ELSE ''
                             END                                 AS dws_act,
                             
                     CASE
                         WHEN (src.id is not null)
                             THEN src.id
                         WHEN (mirror.id is not null)
                             THEN mirror.id
                         ELSE null
                     END AS id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.removal_date
                         WHEN (mirror.id is not null)
                             THEN mirror.removal_date
                         ELSE null
                     END AS removal_date
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.removed
                         WHEN (mirror.id is not null)
                             THEN mirror.removed
                         ELSE null
                     END AS removed
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.code
                         WHEN (mirror.id is not null)
                             THEN mirror.code
                         ELSE null
                     END AS code
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.color
                         WHEN (mirror.id is not null)
                             THEN mirror.color
                         ELSE null
                     END AS color
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.folder
                         WHEN (mirror.id is not null)
                             THEN mirror.folder
                         ELSE null
                     END AS folder
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.pos
                         WHEN (mirror.id is not null)
                             THEN mirror.pos
                         ELSE null
                     END AS pos
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.parent
                         WHEN (mirror.id is not null)
                             THEN mirror.parent
                         ELSE null
                     END AS parent
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.title_en
                         WHEN (mirror.id is not null)
                             THEN mirror.title_en
                         ELSE null
                     END AS title_en
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.title_client
                         WHEN (mirror.id is not null)
                             THEN mirror.title_client
                         ELSE null
                     END AS title_client
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.title
                         WHEN (mirror.id is not null)
                             THEN mirror.title
                         ELSE null
                     END AS title
        
                      FROM ${ods_schema_name}.tbl_impact_src AS src
                      FULL OUTER JOIN ${ods_schema_name}.tbl_impact 
                      AS mirror ON (src.id = mirror.id)) delta WHERE delta.dws_act <> "") delta_num ) rsl

                INSERT INTO TABLE ${ods_schema_name}.tbl_impact_delta partition (`date`)
                    SELECT dws_job, dws_act, nk, dws_uniact,
                    id, removal_date, removed, code, color, folder, pos, parent, title_en, title_client, title, 
                    insert_date, '${date}' as `date`
                WHERE row_num = 1

                INSERT INTO TABLE ${ods_schema_name}.tbl_impact_double
                    SELECT dws_job,
                    insert_date as as_of_date,
                    CASE 
                        WHEN (row_num = 1) THEN 'Y'
                    ELSE 'N' END AS effective_flag,
                    id, removal_date, removed, code, color, folder, pos, parent, title_en, title_client, title
                    WHERE count_num > 1;