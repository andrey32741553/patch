FROM (SELECT *, 
                      count(id) over (partition by id order by insert_date desc) as count_num,
                      row_number() over (partition by id order by insert_date desc) as row_num FROM
                (SELECT 
                    dws_job, insert_date, dws_act, nk, dws_uniact,
                    id, creation_date, last_modified_date, removal_date, removed, archqueue, author_id, case_id, cimodel, configitem_activeaudit, configitem_existstatelog, countcimass, datechcl, datechrep, datefinishexp, dateofservprov, datestartexp, dnsname, headplace, headslmservice, idholder, invunit_monitoringdate, invunitqueue, ip, lastauddate, lastcioffdate, lastclosedate, location, mac, num_, parent, prefix, responsibleemployee_id, responsiblestarttime, responsibleteam_id, sn, sourcecall, state, statestarttime, supservice, supsrvtech, title, vendor
                FROM (SELECT coalesce(mirror.nk, reflect("java.util.UUID", "randomUUID")) AS nk,
                             '${dws_job}'                                AS dws_job,
                             '${insert_date}'                              AS insert_date,
                             'N'                                     AS dws_uniact,
                             CASE
                                 WHEN ((mirror.id is null) or (mirror.id = src.id and mirror.deleted_flag = 'Y' and '${insert_date}' > mirror.insert_date))
                                     THEN 'I'
                                 WHEN (src.id is null and mirror.deleted_flag = 'N')
                                     THEN 'D'
                                 WHEN (mirror.deleted_flag = 'N') and ((src.creation_date <=> mirror.creation_date) = FALSE or (src.last_modified_date <=> mirror.last_modified_date) = FALSE or (src.removal_date <=> mirror.removal_date) = FALSE or (src.removed <=> mirror.removed) = FALSE or (src.archqueue <=> mirror.archqueue) = FALSE or (src.author_id <=> mirror.author_id) = FALSE or (src.case_id <=> mirror.case_id) = FALSE or (src.cimodel <=> mirror.cimodel) = FALSE or (src.configitem_activeaudit <=> mirror.configitem_activeaudit) = FALSE or (src.configitem_existstatelog <=> mirror.configitem_existstatelog) = FALSE or (src.countcimass <=> mirror.countcimass) = FALSE or (src.datechcl <=> mirror.datechcl) = FALSE or (src.datechrep <=> mirror.datechrep) = FALSE or (src.datefinishexp <=> mirror.datefinishexp) = FALSE or (src.dateofservprov <=> mirror.dateofservprov) = FALSE or (src.datestartexp <=> mirror.datestartexp) = FALSE or (src.dnsname <=> mirror.dnsname) = FALSE or (src.headplace <=> mirror.headplace) = FALSE or (src.headslmservice <=> mirror.headslmservice) = FALSE or (src.idholder <=> mirror.idholder) = FALSE or (src.invunit_monitoringdate <=> mirror.invunit_monitoringdate) = FALSE or (src.invunitqueue <=> mirror.invunitqueue) = FALSE or (src.ip <=> mirror.ip) = FALSE or (src.lastauddate <=> mirror.lastauddate) = FALSE or (src.lastcioffdate <=> mirror.lastcioffdate) = FALSE or (src.lastclosedate <=> mirror.lastclosedate) = FALSE or (src.location <=> mirror.location) = FALSE or (src.mac <=> mirror.mac) = FALSE or (src.num_ <=> mirror.num_) = FALSE or (src.parent <=> mirror.parent) = FALSE or (src.prefix <=> mirror.prefix) = FALSE or (src.responsibleemployee_id <=> mirror.responsibleemployee_id) = FALSE or (src.responsiblestarttime <=> mirror.responsiblestarttime) = FALSE or (src.responsibleteam_id <=> mirror.responsibleteam_id) = FALSE or (src.sn <=> mirror.sn) = FALSE or (src.sourcecall <=> mirror.sourcecall) = FALSE or (src.state <=> mirror.state) = FALSE or (src.statestarttime <=> mirror.statestarttime) = FALSE or (src.supservice <=> mirror.supservice) = FALSE or (src.supsrvtech <=> mirror.supsrvtech) = FALSE or (src.title <=> mirror.title) = FALSE or (src.vendor <=> mirror.vendor) = FALSE) 
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
                             THEN src.archqueue
                         WHEN (mirror.id is not null)
                             THEN mirror.archqueue
                         ELSE null
                     END AS archqueue
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
                             THEN src.case_id
                         WHEN (mirror.id is not null)
                             THEN mirror.case_id
                         ELSE null
                     END AS case_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.cimodel
                         WHEN (mirror.id is not null)
                             THEN mirror.cimodel
                         ELSE null
                     END AS cimodel
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.configitem_activeaudit
                         WHEN (mirror.id is not null)
                             THEN mirror.configitem_activeaudit
                         ELSE null
                     END AS configitem_activeaudit
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.configitem_existstatelog
                         WHEN (mirror.id is not null)
                             THEN mirror.configitem_existstatelog
                         ELSE null
                     END AS configitem_existstatelog
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.countcimass
                         WHEN (mirror.id is not null)
                             THEN mirror.countcimass
                         ELSE null
                     END AS countcimass
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.datechcl
                         WHEN (mirror.id is not null)
                             THEN mirror.datechcl
                         ELSE null
                     END AS datechcl
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.datechrep
                         WHEN (mirror.id is not null)
                             THEN mirror.datechrep
                         ELSE null
                     END AS datechrep
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.datefinishexp
                         WHEN (mirror.id is not null)
                             THEN mirror.datefinishexp
                         ELSE null
                     END AS datefinishexp
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.dateofservprov
                         WHEN (mirror.id is not null)
                             THEN mirror.dateofservprov
                         ELSE null
                     END AS dateofservprov
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.datestartexp
                         WHEN (mirror.id is not null)
                             THEN mirror.datestartexp
                         ELSE null
                     END AS datestartexp
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.dnsname
                         WHEN (mirror.id is not null)
                             THEN mirror.dnsname
                         ELSE null
                     END AS dnsname
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.headplace
                         WHEN (mirror.id is not null)
                             THEN mirror.headplace
                         ELSE null
                     END AS headplace
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.headslmservice
                         WHEN (mirror.id is not null)
                             THEN mirror.headslmservice
                         ELSE null
                     END AS headslmservice
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
                             THEN src.invunit_monitoringdate
                         WHEN (mirror.id is not null)
                             THEN mirror.invunit_monitoringdate
                         ELSE null
                     END AS invunit_monitoringdate
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.invunitqueue
                         WHEN (mirror.id is not null)
                             THEN mirror.invunitqueue
                         ELSE null
                     END AS invunitqueue
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.ip
                         WHEN (mirror.id is not null)
                             THEN mirror.ip
                         ELSE null
                     END AS ip
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.lastauddate
                         WHEN (mirror.id is not null)
                             THEN mirror.lastauddate
                         ELSE null
                     END AS lastauddate
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.lastcioffdate
                         WHEN (mirror.id is not null)
                             THEN mirror.lastcioffdate
                         ELSE null
                     END AS lastcioffdate
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.lastclosedate
                         WHEN (mirror.id is not null)
                             THEN mirror.lastclosedate
                         ELSE null
                     END AS lastclosedate
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
                             THEN src.mac
                         WHEN (mirror.id is not null)
                             THEN mirror.mac
                         ELSE null
                     END AS mac
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
                             THEN src.parent
                         WHEN (mirror.id is not null)
                             THEN mirror.parent
                         ELSE null
                     END AS parent
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
                             THEN src.responsibleemployee_id
                         WHEN (mirror.id is not null)
                             THEN mirror.responsibleemployee_id
                         ELSE null
                     END AS responsibleemployee_id
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
                             THEN src.responsibleteam_id
                         WHEN (mirror.id is not null)
                             THEN mirror.responsibleteam_id
                         ELSE null
                     END AS responsibleteam_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.sn
                         WHEN (mirror.id is not null)
                             THEN mirror.sn
                         ELSE null
                     END AS sn
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.sourcecall
                         WHEN (mirror.id is not null)
                             THEN mirror.sourcecall
                         ELSE null
                     END AS sourcecall
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
                             THEN src.supservice
                         WHEN (mirror.id is not null)
                             THEN mirror.supservice
                         ELSE null
                     END AS supservice
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.supsrvtech
                         WHEN (mirror.id is not null)
                             THEN mirror.supsrvtech
                         ELSE null
                     END AS supsrvtech
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
                             THEN src.vendor
                         WHEN (mirror.id is not null)
                             THEN mirror.vendor
                         ELSE null
                     END AS vendor
        
                      FROM ${ods_schema_name}.tbl_objectbase_src AS src
                      FULL OUTER JOIN ${ods_schema_name}.tbl_objectbase 
                      AS mirror ON (src.id = mirror.id)) delta WHERE delta.dws_act <> "") delta_num ) rsl

                INSERT INTO TABLE ${ods_schema_name}.tbl_objectbase_delta partition (`date`)
                    SELECT dws_job, dws_act, nk, dws_uniact,
                    id, creation_date, last_modified_date, removal_date, removed, archqueue, author_id, case_id, cimodel, configitem_activeaudit, configitem_existstatelog, countcimass, datechcl, datechrep, datefinishexp, dateofservprov, datestartexp, dnsname, headplace, headslmservice, idholder, invunit_monitoringdate, invunitqueue, ip, lastauddate, lastcioffdate, lastclosedate, location, mac, num_, parent, prefix, responsibleemployee_id, responsiblestarttime, responsibleteam_id, sn, sourcecall, state, statestarttime, supservice, supsrvtech, title, vendor, 
                    insert_date, '${date}' as `date`
                WHERE row_num = 1

                INSERT INTO TABLE ${ods_schema_name}.tbl_objectbase_double
                    SELECT dws_job,
                    insert_date as as_of_date,
                    CASE 
                        WHEN (row_num = 1) THEN 'Y'
                    ELSE 'N' END AS effective_flag,
                    id, creation_date, last_modified_date, removal_date, removed, archqueue, author_id, case_id, cimodel, configitem_activeaudit, configitem_existstatelog, countcimass, datechcl, datechrep, datefinishexp, dateofservprov, datestartexp, dnsname, headplace, headslmservice, idholder, invunit_monitoringdate, invunitqueue, ip, lastauddate, lastcioffdate, lastclosedate, location, mac, num_, parent, prefix, responsibleemployee_id, responsiblestarttime, responsibleteam_id, sn, sourcecall, state, statestarttime, supservice, supsrvtech, title, vendor
                    WHERE count_num > 1;