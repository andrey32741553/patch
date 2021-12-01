FROM (SELECT *, 
                      count(id) over (partition by id order by insert_date desc) as count_num,
                      row_number() over (partition by id order by insert_date desc) as row_num FROM
                (SELECT 
                    dws_job, insert_date, dws_act, nk, dws_uniact,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, num_, responsiblestarttime, state, statestarttime, inventorynum, description, groupexprfc, groupdevman, groupregrfcman, idholder, parent, groupitservman, techservice_provider, author_id, system_icon_id, responsibleemployee_id, responsibleteam_id, emailsc, compcheck, groupbisclient, groupresprfc, negotstaddtkt, defstart, resippolice, rtfallow, grpallow, locallow, grpdesc, flallow, ciallow, prefix, canchecksn, monitoringlink, archiveaction, stabilmangroup, mangroupinv, assetpolicy, usericon, routcinotfound, groupregisprb, prbregispolicy
                FROM (SELECT coalesce(mirror.nk, reflect("java.util.UUID", "randomUUID")) AS nk,
                             '${dws_job}'                                AS dws_job,
                             '${insert_date}'                              AS insert_date,
                             'N'                                     AS dws_uniact,
                             CASE
                                 WHEN ((mirror.id is null) or (mirror.id = src.id and mirror.deleted_flag = 'Y' and '${insert_date}' > mirror.insert_date))
                                     THEN 'I'
                                 WHEN (src.id is null and mirror.deleted_flag = 'N')
                                     THEN 'D'
                                 WHEN (mirror.deleted_flag = 'N') and ((src.creation_date <=> mirror.creation_date) = FALSE or (src.last_modified_date <=> mirror.last_modified_date) = FALSE or (src.removal_date <=> mirror.removal_date) = FALSE or (src.removed <=> mirror.removed) = FALSE or (src.title <=> mirror.title) = FALSE or (src.case_id <=> mirror.case_id) = FALSE or (src.num_ <=> mirror.num_) = FALSE or (src.responsiblestarttime <=> mirror.responsiblestarttime) = FALSE or (src.state <=> mirror.state) = FALSE or (src.statestarttime <=> mirror.statestarttime) = FALSE or (src.inventorynum <=> mirror.inventorynum) = FALSE or (src.description <=> mirror.description) = FALSE or (src.groupexprfc <=> mirror.groupexprfc) = FALSE or (src.groupdevman <=> mirror.groupdevman) = FALSE or (src.groupregrfcman <=> mirror.groupregrfcman) = FALSE or (src.idholder <=> mirror.idholder) = FALSE or (src.parent <=> mirror.parent) = FALSE or (src.groupitservman <=> mirror.groupitservman) = FALSE or (src.techservice_provider <=> mirror.techservice_provider) = FALSE or (src.author_id <=> mirror.author_id) = FALSE or (src.system_icon_id <=> mirror.system_icon_id) = FALSE or (src.responsibleemployee_id <=> mirror.responsibleemployee_id) = FALSE or (src.responsibleteam_id <=> mirror.responsibleteam_id) = FALSE or (src.emailsc <=> mirror.emailsc) = FALSE or (src.compcheck <=> mirror.compcheck) = FALSE or (src.groupbisclient <=> mirror.groupbisclient) = FALSE or (src.groupresprfc <=> mirror.groupresprfc) = FALSE or (src.negotstaddtkt <=> mirror.negotstaddtkt) = FALSE or (src.defstart <=> mirror.defstart) = FALSE or (src.resippolice <=> mirror.resippolice) = FALSE or (src.rtfallow <=> mirror.rtfallow) = FALSE or (src.grpallow <=> mirror.grpallow) = FALSE or (src.locallow <=> mirror.locallow) = FALSE or (src.grpdesc <=> mirror.grpdesc) = FALSE or (src.flallow <=> mirror.flallow) = FALSE or (src.ciallow <=> mirror.ciallow) = FALSE or (src.prefix <=> mirror.prefix) = FALSE or (src.canchecksn <=> mirror.canchecksn) = FALSE or (src.monitoringlink <=> mirror.monitoringlink) = FALSE or (src.archiveaction <=> mirror.archiveaction) = FALSE or (src.stabilmangroup <=> mirror.stabilmangroup) = FALSE or (src.mangroupinv <=> mirror.mangroupinv) = FALSE or (src.assetpolicy <=> mirror.assetpolicy) = FALSE or (src.usericon <=> mirror.usericon) = FALSE or (src.routcinotfound <=> mirror.routcinotfound) = FALSE or (src.groupregisprb <=> mirror.groupregisprb) = FALSE or (src.prbregispolicy <=> mirror.prbregispolicy) = FALSE) 
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
                             THEN src.responsiblestarttime
                         WHEN (mirror.id is not null)
                             THEN mirror.responsiblestarttime
                         ELSE null
                     END AS responsiblestarttime
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.state
                         WHEN (mirror.id is not null)
                             THEN mirror.state
                         ELSE null
                     END AS state
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.statestarttime
                         WHEN (mirror.id is not null)
                             THEN mirror.statestarttime
                         ELSE null
                     END AS statestarttime
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.inventorynum
                         WHEN (mirror.id is not null)
                             THEN mirror.inventorynum
                         ELSE null
                     END AS inventorynum
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
                             THEN src.groupexprfc
                         WHEN (mirror.id is not null)
                             THEN mirror.groupexprfc
                         ELSE null
                     END AS groupexprfc
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.groupdevman
                         WHEN (mirror.id is not null)
                             THEN mirror.groupdevman
                         ELSE null
                     END AS groupdevman
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.groupregrfcman
                         WHEN (mirror.id is not null)
                             THEN mirror.groupregrfcman
                         ELSE null
                     END AS groupregrfcman
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
                             THEN src.parent
                         WHEN (mirror.id is not null)
                             THEN mirror.parent
                         ELSE null
                     END AS parent
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.groupitservman
                         WHEN (mirror.id is not null)
                             THEN mirror.groupitservman
                         ELSE null
                     END AS groupitservman
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.techservice_provider
                         WHEN (mirror.id is not null)
                             THEN mirror.techservice_provider
                         ELSE null
                     END AS techservice_provider
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
                             THEN src.responsibleemployee_id
                         WHEN (mirror.id is not null)
                             THEN mirror.responsibleemployee_id
                         ELSE null
                     END AS responsibleemployee_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.responsibleteam_id
                         WHEN (mirror.id is not null)
                             THEN mirror.responsibleteam_id
                         ELSE null
                     END AS responsibleteam_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.emailsc
                         WHEN (mirror.id is not null)
                             THEN mirror.emailsc
                         ELSE null
                     END AS emailsc
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.compcheck
                         WHEN (mirror.id is not null)
                             THEN mirror.compcheck
                         ELSE null
                     END AS compcheck
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.groupbisclient
                         WHEN (mirror.id is not null)
                             THEN mirror.groupbisclient
                         ELSE null
                     END AS groupbisclient
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.groupresprfc
                         WHEN (mirror.id is not null)
                             THEN mirror.groupresprfc
                         ELSE null
                     END AS groupresprfc
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.negotstaddtkt
                         WHEN (mirror.id is not null)
                             THEN mirror.negotstaddtkt
                         ELSE null
                     END AS negotstaddtkt
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.defstart
                         WHEN (mirror.id is not null)
                             THEN mirror.defstart
                         ELSE null
                     END AS defstart
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.resippolice
                         WHEN (mirror.id is not null)
                             THEN mirror.resippolice
                         ELSE null
                     END AS resippolice
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.rtfallow
                         WHEN (mirror.id is not null)
                             THEN mirror.rtfallow
                         ELSE null
                     END AS rtfallow
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.grpallow
                         WHEN (mirror.id is not null)
                             THEN mirror.grpallow
                         ELSE null
                     END AS grpallow
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.locallow
                         WHEN (mirror.id is not null)
                             THEN mirror.locallow
                         ELSE null
                     END AS locallow
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.grpdesc
                         WHEN (mirror.id is not null)
                             THEN mirror.grpdesc
                         ELSE null
                     END AS grpdesc
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.flallow
                         WHEN (mirror.id is not null)
                             THEN mirror.flallow
                         ELSE null
                     END AS flallow
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.ciallow
                         WHEN (mirror.id is not null)
                             THEN mirror.ciallow
                         ELSE null
                     END AS ciallow
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.prefix
                         WHEN (mirror.id is not null)
                             THEN mirror.prefix
                         ELSE null
                     END AS prefix
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.canchecksn
                         WHEN (mirror.id is not null)
                             THEN mirror.canchecksn
                         ELSE null
                     END AS canchecksn
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.monitoringlink
                         WHEN (mirror.id is not null)
                             THEN mirror.monitoringlink
                         ELSE null
                     END AS monitoringlink
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.archiveaction
                         WHEN (mirror.id is not null)
                             THEN mirror.archiveaction
                         ELSE null
                     END AS archiveaction
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.stabilmangroup
                         WHEN (mirror.id is not null)
                             THEN mirror.stabilmangroup
                         ELSE null
                     END AS stabilmangroup
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.mangroupinv
                         WHEN (mirror.id is not null)
                             THEN mirror.mangroupinv
                         ELSE null
                     END AS mangroupinv
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.assetpolicy
                         WHEN (mirror.id is not null)
                             THEN mirror.assetpolicy
                         ELSE null
                     END AS assetpolicy
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.usericon
                         WHEN (mirror.id is not null)
                             THEN mirror.usericon
                         ELSE null
                     END AS usericon
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.routcinotfound
                         WHEN (mirror.id is not null)
                             THEN mirror.routcinotfound
                         ELSE null
                     END AS routcinotfound
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.groupregisprb
                         WHEN (mirror.id is not null)
                             THEN mirror.groupregisprb
                         ELSE null
                     END AS groupregisprb
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.prbregispolicy
                         WHEN (mirror.id is not null)
                             THEN mirror.prbregispolicy
                         ELSE null
                     END AS prbregispolicy
        
                      FROM ${ods_schema_name}.tbl_service_src AS src
                      FULL OUTER JOIN ${ods_schema_name}.tbl_service 
                      AS mirror ON (src.id = mirror.id)) delta WHERE delta.dws_act <> "") delta_num ) rsl

                INSERT INTO TABLE ${ods_schema_name}.tbl_service_delta partition (`date`)
                    SELECT dws_job, dws_act, nk, dws_uniact,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, num_, responsiblestarttime, state, statestarttime, inventorynum, description, groupexprfc, groupdevman, groupregrfcman, idholder, parent, groupitservman, techservice_provider, author_id, system_icon_id, responsibleemployee_id, responsibleteam_id, emailsc, compcheck, groupbisclient, groupresprfc, negotstaddtkt, defstart, resippolice, rtfallow, grpallow, locallow, grpdesc, flallow, ciallow, prefix, canchecksn, monitoringlink, archiveaction, stabilmangroup, mangroupinv, assetpolicy, usericon, routcinotfound, groupregisprb, prbregispolicy, 
                    insert_date, '${date}' as `date`
                WHERE row_num = 1

                INSERT INTO TABLE ${ods_schema_name}.tbl_service_double
                    SELECT dws_job,
                    insert_date as as_of_date,
                    CASE 
                        WHEN (row_num = 1) THEN 'Y'
                    ELSE 'N' END AS effective_flag,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, num_, responsiblestarttime, state, statestarttime, inventorynum, description, groupexprfc, groupdevman, groupregrfcman, idholder, parent, groupitservman, techservice_provider, author_id, system_icon_id, responsibleemployee_id, responsibleteam_id, emailsc, compcheck, groupbisclient, groupresprfc, negotstaddtkt, defstart, resippolice, rtfallow, grpallow, locallow, grpdesc, flallow, ciallow, prefix, canchecksn, monitoringlink, archiveaction, stabilmangroup, mangroupinv, assetpolicy, usericon, routcinotfound, groupregisprb, prbregispolicy
                    WHERE count_num > 1;