FROM (SELECT *, 
                      count(id) over (partition by id order by insert_date desc) as count_num,
                      row_number() over (partition by id order by insert_date desc) as row_num FROM
                (SELECT 
                    dws_job, insert_date, dws_act, nk, dws_uniact,
                    id, creation_date, last_modified_date, removal_date, removed, case_id, num_, author_id, system_icon_id, headou_id, leader_id, teamname, description, grouprule, code, dutygroup, maingroup, headgroup, role_linktoprocess, title
                FROM (SELECT coalesce(mirror.nk, reflect("java.util.UUID", "randomUUID")) AS nk,
                             '${dws_job}'                                AS dws_job,
                             '${insert_date}'                              AS insert_date,
                             'N'                                     AS dws_uniact,
                             CASE
                                 WHEN ((mirror.id is null) or (mirror.id = src.id and mirror.deleted_flag = 'Y' and '${insert_date}' > mirror.insert_date))
                                     THEN 'I'
                                 WHEN (src.id is null and mirror.deleted_flag = 'N')
                                     THEN 'D'
                                 WHEN (mirror.deleted_flag = 'N') and ((src.creation_date <=> mirror.creation_date) = FALSE or (src.last_modified_date <=> mirror.last_modified_date) = FALSE or (src.removal_date <=> mirror.removal_date) = FALSE or (src.removed <=> mirror.removed) = FALSE or (src.case_id <=> mirror.case_id) = FALSE or (src.num_ <=> mirror.num_) = FALSE or (src.author_id <=> mirror.author_id) = FALSE or (src.system_icon_id <=> mirror.system_icon_id) = FALSE or (src.headou_id <=> mirror.headou_id) = FALSE or (src.leader_id <=> mirror.leader_id) = FALSE or (src.teamname <=> mirror.teamname) = FALSE or (src.description <=> mirror.description) = FALSE or (src.grouprule <=> mirror.grouprule) = FALSE or (src.code <=> mirror.code) = FALSE or (src.dutygroup <=> mirror.dutygroup) = FALSE or (src.maingroup <=> mirror.maingroup) = FALSE or (src.headgroup <=> mirror.headgroup) = FALSE or (src.role_linktoprocess <=> mirror.role_linktoprocess) = FALSE or (src.title <=> mirror.title) = FALSE) 
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
                             THEN src.headou_id
                         WHEN (mirror.id is not null)
                             THEN mirror.headou_id
                         ELSE null
                     END AS headou_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.leader_id
                         WHEN (mirror.id is not null)
                             THEN mirror.leader_id
                         ELSE null
                     END AS leader_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.teamname
                         WHEN (mirror.id is not null)
                             THEN mirror.teamname
                         ELSE null
                     END AS teamname
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.description
                         WHEN (mirror.id is not null)
                             THEN mirror.description
                         ELSE null
                     END AS description
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.grouprule
                         WHEN (mirror.id is not null)
                             THEN mirror.grouprule
                         ELSE null
                     END AS grouprule
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
                             THEN src.dutygroup
                         WHEN (mirror.id is not null)
                             THEN mirror.dutygroup
                         ELSE null
                     END AS dutygroup
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.maingroup
                         WHEN (mirror.id is not null)
                             THEN mirror.maingroup
                         ELSE null
                     END AS maingroup
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.headgroup
                         WHEN (mirror.id is not null)
                             THEN mirror.headgroup
                         ELSE null
                     END AS headgroup
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.role_linktoprocess
                         WHEN (mirror.id is not null)
                             THEN mirror.role_linktoprocess
                         ELSE null
                     END AS role_linktoprocess
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.title
                         WHEN (mirror.id is not null)
                             THEN mirror.title
                         ELSE null
                     END AS title
        
                      FROM ${ods_schema_name}.tbl_team_src AS src
                      FULL OUTER JOIN ${ods_schema_name}.tbl_team 
                      AS mirror ON (src.id = mirror.id)) delta WHERE delta.dws_act <> "") delta_num ) rsl

                INSERT INTO TABLE ${ods_schema_name}.tbl_team_delta partition (`date`)
                    SELECT dws_job, dws_act, nk, dws_uniact,
                    id, creation_date, last_modified_date, removal_date, removed, case_id, num_, author_id, system_icon_id, headou_id, leader_id, teamname, description, grouprule, code, dutygroup, maingroup, headgroup, role_linktoprocess, title, 
                    insert_date, '${date}' as `date`
                WHERE row_num = 1

                INSERT INTO TABLE ${ods_schema_name}.tbl_team_double
                    SELECT dws_job,
                    insert_date as as_of_date,
                    CASE 
                        WHEN (row_num = 1) THEN 'Y'
                    ELSE 'N' END AS effective_flag,
                    id, creation_date, last_modified_date, removal_date, removed, case_id, num_, author_id, system_icon_id, headou_id, leader_id, teamname, description, grouprule, code, dutygroup, maingroup, headgroup, role_linktoprocess, title
                    WHERE count_num > 1;