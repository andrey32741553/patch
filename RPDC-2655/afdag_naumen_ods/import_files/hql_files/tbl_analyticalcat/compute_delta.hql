FROM (SELECT *,
             count(id) over (partition by id order by insert_date desc)    as count_num,
             row_number() over (partition by id order by insert_date desc) as row_num
      FROM (SELECT dws_job,
                   insert_date,
                   dws_act,
                   nk,
                   dws_uniact,
                   id,
                   creation_date,
                   last_modified_date,
                   removal_date,
                   removed,
                   title,
                   case_id,
                   num_,
                   responsiblestarttime,
                   state,
                   statestarttime,
                   parent,
                   author_id,
                   system_icon_id,
                   responsibleemployee_id,
                   responsibleteam_id,
                   headcat,
                   visor,
                   catalog_needcheck,
                   catalog_needcopy
            FROM (SELECT coalesce(mirror.nk, reflect("java.util.UUID", "randomUUID")) AS nk,
                         '${dws_job}'                                                 AS dws_job,
                         '${insert_date}'                                             AS insert_date,
                         'N'                                                          AS dws_uniact,
                         CASE
                             WHEN ((mirror.id is null) or (mirror.id = src.id and mirror.deleted_flag = 'Y' and
                                                           '${insert_date}' > mirror.insert_date))
                                 THEN 'I'
                             WHEN (src.id is null and mirror.deleted_flag = 'N')
                                 THEN 'D'
                             WHEN (mirror.deleted_flag = 'N') and
                                  ((src.creation_date <=> mirror.creation_date) = FALSE or
                                   (src.last_modified_date <=> mirror.last_modified_date) = FALSE or
                                   (src.removal_date <=> mirror.removal_date) = FALSE or
                                   (src.removed <=> mirror.removed) = FALSE or (src.title <=> mirror.title) = FALSE or
                                   (src.case_id <=> mirror.case_id) = FALSE or (src.num_ <=> mirror.num_) = FALSE or
                                   (src.responsiblestarttime <=> mirror.responsiblestarttime) = FALSE or
                                   (src.state <=> mirror.state) = FALSE or
                                   (src.statestarttime <=> mirror.statestarttime) = FALSE or
                                   (src.parent <=> mirror.parent) = FALSE or
                                   (src.author_id <=> mirror.author_id) = FALSE or
                                   (src.system_icon_id <=> mirror.system_icon_id) = FALSE or
                                   (src.responsibleemployee_id <=> mirror.responsibleemployee_id) = FALSE or
                                   (src.responsibleteam_id <=> mirror.responsibleteam_id) = FALSE or
                                   (src.headcat <=> mirror.headcat) = FALSE or (src.visor <=> mirror.visor) = FALSE or
                                   (src.catalog_needcheck <=> mirror.catalog_needcheck) = FALSE or
                                   (src.catalog_needcopy <=> mirror.catalog_needcopy) = FALSE)
                                 THEN 'U'
                             ELSE ''
                             END                                                      AS dws_act,

                         CASE
                             WHEN (src.id is not null)
                                 THEN src.id
                             WHEN (mirror.id is not null)
                                 THEN mirror.id
                             ELSE null
                             END                                                      AS id
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.creation_date
                             WHEN (mirror.id is not null)
                                 THEN mirror.creation_date
                             ELSE null
                             END                                                      AS creation_date
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.last_modified_date
                             WHEN (mirror.id is not null)
                                 THEN mirror.last_modified_date
                             ELSE null
                             END                                                      AS last_modified_date
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.removal_date
                             WHEN (mirror.id is not null)
                                 THEN mirror.removal_date
                             ELSE null
                             END                                                      AS removal_date
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.removed
                             WHEN (mirror.id is not null)
                                 THEN mirror.removed
                             ELSE null
                             END                                                      AS removed
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.title
                             WHEN (mirror.id is not null)
                                 THEN mirror.title
                             ELSE null
                             END                                                      AS title
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.case_id
                             WHEN (mirror.id is not null)
                                 THEN mirror.case_id
                             ELSE null
                             END                                                      AS case_id
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.num_
                             WHEN (mirror.id is not null)
                                 THEN mirror.num_
                             ELSE null
                             END                                                      AS num_
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.responsiblestarttime
                             WHEN (mirror.id is not null)
                                 THEN mirror.responsiblestarttime
                             ELSE null
                             END                                                      AS responsiblestarttime
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.state
                             WHEN (mirror.id is not null)
                                 THEN mirror.state
                             ELSE null
                             END                                                      AS state
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.statestarttime
                             WHEN (mirror.id is not null)
                                 THEN mirror.statestarttime
                             ELSE null
                             END                                                      AS statestarttime
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.parent
                             WHEN (mirror.id is not null)
                                 THEN mirror.parent
                             ELSE null
                             END                                                      AS parent
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.author_id
                             WHEN (mirror.id is not null)
                                 THEN mirror.author_id
                             ELSE null
                             END                                                      AS author_id
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.system_icon_id
                             WHEN (mirror.id is not null)
                                 THEN mirror.system_icon_id
                             ELSE null
                             END                                                      AS system_icon_id
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.responsibleemployee_id
                             WHEN (mirror.id is not null)
                                 THEN mirror.responsibleemployee_id
                             ELSE null
                             END                                                      AS responsibleemployee_id
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.responsibleteam_id
                             WHEN (mirror.id is not null)
                                 THEN mirror.responsibleteam_id
                             ELSE null
                             END                                                      AS responsibleteam_id
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.headcat
                             WHEN (mirror.id is not null)
                                 THEN mirror.headcat
                             ELSE null
                             END                                                      AS headcat
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.visor
                             WHEN (mirror.id is not null)
                                 THEN mirror.visor
                             ELSE null
                             END                                                      AS visor
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.catalog_needcheck
                             WHEN (mirror.id is not null)
                                 THEN mirror.catalog_needcheck
                             ELSE null
                             END                                                      AS catalog_needcheck
                          ,
                         CASE
                             WHEN (src.id is not null)
                                 THEN src.catalog_needcopy
                             WHEN (mirror.id is not null)
                                 THEN mirror.catalog_needcopy
                             ELSE null
                             END                                                      AS catalog_needcopy

                  FROM ${ods_schema_name}.tbl_analyticalcat_src AS src
                           FULL OUTER JOIN ${ods_schema_name}.tbl_analyticalcat
                      AS mirror ON (src.id = mirror.id)) delta
            WHERE delta.dws_act <> "") delta_num) rsl

INSERT
INTO TABLE ${ods_schema_name}.tbl_analyticalcat_delta partition (`date`)
SELECT dws_job,
       dws_act,
       nk,
       dws_uniact,
       id,
       creation_date,
       last_modified_date,
       removal_date,
       removed,
       title,
       case_id,
       num_,
       responsiblestarttime,
       state,
       statestarttime,
       parent,
       author_id,
       system_icon_id,
       responsibleemployee_id,
       responsibleteam_id,
       headcat,
       visor,
       catalog_needcheck,
       catalog_needcopy,
       insert_date,
       '${date}' as `date`
WHERE row_num = 1

INSERT
INTO TABLE ${ods_schema_name}.tbl_analyticalcat_double
SELECT dws_job,
       insert_date      as as_of_date,
       CASE
           WHEN (row_num = 1) THEN 'Y'
           ELSE 'N' END AS effective_flag,
       1 as algorithm_ncode,
       id,
       creation_date,
       last_modified_date,
       removal_date,
       removed,
       title,
       case_id,
       num_,
       responsiblestarttime,
       state,
       statestarttime,
       parent,
       author_id,
       system_icon_id,
       responsibleemployee_id,
       responsibleteam_id,
       headcat,
       visor,
       catalog_needcheck,
       catalog_needcopy
WHERE count_num > 1;