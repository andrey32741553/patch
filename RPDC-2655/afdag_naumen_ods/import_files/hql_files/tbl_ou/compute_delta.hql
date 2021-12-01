FROM (SELECT *, 
                      count(id) over (partition by id order by insert_date desc) as count_num,
                      row_number() over (partition by id order by insert_date desc) as row_num FROM
                (SELECT 
                    dws_job, insert_date, dws_act, nk, dws_uniact,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, num_, author_id, system_icon_id, head_id, parent_id, accesskey, namefromad, idholder, shorttitle, accesskeyintrl, ouext_teampartners, location, updated, headou, ouext_syncuser
                FROM (SELECT coalesce(mirror.nk, reflect("java.util.UUID", "randomUUID")) AS nk,
                             '${dws_job}'                                AS dws_job,
                             '${insert_date}'                              AS insert_date,
                             'N'                                     AS dws_uniact,
                             CASE
                                 WHEN ((mirror.id is null) or (mirror.id = src.id and mirror.deleted_flag = 'Y' and '${insert_date}' > mirror.insert_date))
                                     THEN 'I'
                                 WHEN (src.id is null and mirror.deleted_flag = 'N')
                                     THEN 'D'
                                 WHEN (mirror.deleted_flag = 'N') and ((src.creation_date <=> mirror.creation_date) = FALSE or (src.last_modified_date <=> mirror.last_modified_date) = FALSE or (src.removal_date <=> mirror.removal_date) = FALSE or (src.removed <=> mirror.removed) = FALSE or (src.title <=> mirror.title) = FALSE or (src.case_id <=> mirror.case_id) = FALSE or (src.num_ <=> mirror.num_) = FALSE or (src.author_id <=> mirror.author_id) = FALSE or (src.system_icon_id <=> mirror.system_icon_id) = FALSE or (src.head_id <=> mirror.head_id) = FALSE or (src.parent_id <=> mirror.parent_id) = FALSE or (src.accesskey <=> mirror.accesskey) = FALSE or (src.namefromad <=> mirror.namefromad) = FALSE or (src.idholder <=> mirror.idholder) = FALSE or (src.shorttitle <=> mirror.shorttitle) = FALSE or (src.accesskeyintrl <=> mirror.accesskeyintrl) = FALSE or (src.ouext_teampartners <=> mirror.ouext_teampartners) = FALSE or (src.location <=> mirror.location) = FALSE or (src.updated <=> mirror.updated) = FALSE or (src.headou <=> mirror.headou) = FALSE or (src.ouext_syncuser <=> mirror.ouext_syncuser) = FALSE) 
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
                             THEN src.creation_date
                         WHEN (mirror.id is not null)
                             THEN mirror.creation_date
                         ELSE null
                     END AS creation_date
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.last_modified_date
                         WHEN (mirror.id is not null)
                             THEN mirror.last_modified_date
                         ELSE null
                     END AS last_modified_date
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
                             THEN src.title
                         WHEN (mirror.id is not null)
                             THEN mirror.title
                         ELSE null
                     END AS title
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.case_id
                         WHEN (mirror.id is not null)
                             THEN mirror.case_id
                         ELSE null
                     END AS case_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.num_
                         WHEN (mirror.id is not null)
                             THEN mirror.num_
                         ELSE null
                     END AS num_
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.author_id
                         WHEN (mirror.id is not null)
                             THEN mirror.author_id
                         ELSE null
                     END AS author_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.system_icon_id
                         WHEN (mirror.id is not null)
                             THEN mirror.system_icon_id
                         ELSE null
                     END AS system_icon_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.head_id
                         WHEN (mirror.id is not null)
                             THEN mirror.head_id
                         ELSE null
                     END AS head_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.parent_id
                         WHEN (mirror.id is not null)
                             THEN mirror.parent_id
                         ELSE null
                     END AS parent_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.accesskey
                         WHEN (mirror.id is not null)
                             THEN mirror.accesskey
                         ELSE null
                     END AS accesskey
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.namefromad
                         WHEN (mirror.id is not null)
                             THEN mirror.namefromad
                         ELSE null
                     END AS namefromad
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.idholder
                         WHEN (mirror.id is not null)
                             THEN mirror.idholder
                         ELSE null
                     END AS idholder
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.shorttitle
                         WHEN (mirror.id is not null)
                             THEN mirror.shorttitle
                         ELSE null
                     END AS shorttitle
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.accesskeyintrl
                         WHEN (mirror.id is not null)
                             THEN mirror.accesskeyintrl
                         ELSE null
                     END AS accesskeyintrl
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.ouext_teampartners
                         WHEN (mirror.id is not null)
                             THEN mirror.ouext_teampartners
                         ELSE null
                     END AS ouext_teampartners
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.location
                         WHEN (mirror.id is not null)
                             THEN mirror.location
                         ELSE null
                     END AS location
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.updated
                         WHEN (mirror.id is not null)
                             THEN mirror.updated
                         ELSE null
                     END AS updated
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.headou
                         WHEN (mirror.id is not null)
                             THEN mirror.headou
                         ELSE null
                     END AS headou
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.ouext_syncuser
                         WHEN (mirror.id is not null)
                             THEN mirror.ouext_syncuser
                         ELSE null
                     END AS ouext_syncuser
        
                      FROM ${ods_schema_name}.tbl_ou_src AS src
                      FULL OUTER JOIN ${ods_schema_name}.tbl_ou 
                      AS mirror ON (src.id = mirror.id)) delta WHERE delta.dws_act <> "") delta_num ) rsl

                INSERT INTO TABLE ${ods_schema_name}.tbl_ou_delta partition (`date`)
                    SELECT dws_job, dws_act, nk, dws_uniact,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, num_, author_id, system_icon_id, head_id, parent_id, accesskey, namefromad, idholder, shorttitle, accesskeyintrl, ouext_teampartners, location, updated, headou, ouext_syncuser, 
                    insert_date, '${date}' as `date`
                WHERE row_num = 1

                INSERT INTO TABLE ${ods_schema_name}.tbl_ou_double
                    SELECT dws_job,
                    insert_date as as_of_date,
                    CASE 
                        WHEN (row_num = 1) THEN 'Y'
                    ELSE 'N' END AS effective_flag,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, num_, author_id, system_icon_id, head_id, parent_id, accesskey, namefromad, idholder, shorttitle, accesskeyintrl, ouext_teampartners, location, updated, headou, ouext_syncuser
                    WHERE count_num > 1;