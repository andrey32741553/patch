FROM (SELECT *, 
                      count(id) over (partition by id order by insert_date desc) as count_num,
                      row_number() over (partition by id order by insert_date desc) as row_num FROM
                (SELECT 
                    dws_job, insert_date, dws_act,
                    id, creation_date, last_modified_date, removal_date, removed, author_id, case_id, ci, client_email, client_name, clientou_id, datedecision, deadLineDate_d, deadlinetime, descriptioninrtf, facttime, impact_id, linkedsc, location, manualclosdate, mark, numreclass, numref, priority_id, proccodeclose, registration_date, resolutiontime, responsibleemployee_id, responsibleteam_id, resumptionsum, route, servicetime_id, sevicecomp, slastop, slmservice, solution, solvedbyemployee_id, solvedbyteam_id, state, statestarttime, stdonechildsc, techdesc, timeclosing24, timezone_id, title, urgency_id, usercalls_reaction1line_l, usercalls_reaction2line_l, usercalls_reaction3line_l, usercalls_reaction4line_l, usercalls_timetorectific_l, usercalls_timework1line_l, usercalls_timework2line_l, usercalls_timework3line_l, usercalls_timework4line_l, username, wayaddressing, whoclose, whodocontr_em, overduestate_s, overduestate_d
                FROM (SELECT 
                             '${dws_job}'                                AS dws_job,
                             '${insert_date}'                              AS insert_date,
                             
                             CASE
                                 WHEN ((mirror.id is null) or (mirror.id = src.id and mirror.deleted_flag = 'Y' and '${insert_date}' > mirror.insert_date))
                                     THEN 'I'
                                 WHEN (src.id is null and mirror.deleted_flag = 'N')
                                     THEN 'D'
                                 WHEN (mirror.deleted_flag = 'N') and ((src.creation_date <=> mirror.creation_date) = FALSE or (src.last_modified_date <=> mirror.last_modified_date) = FALSE or (src.removal_date <=> mirror.removal_date) = FALSE or (src.removed <=> mirror.removed) = FALSE or (src.author_id <=> mirror.author_id) = FALSE or (src.case_id <=> mirror.case_id) = FALSE or (src.ci <=> mirror.ci) = FALSE or (src.client_email <=> mirror.client_email) = FALSE or (src.client_name <=> mirror.client_name) = FALSE or (src.clientou_id <=> mirror.clientou_id) = FALSE or (src.datedecision <=> mirror.datedecision) = FALSE or (src.deadLineDate_d <=> mirror.deadLineDate_d) = FALSE or (src.deadlinetime <=> mirror.deadlinetime) = FALSE or (src.descriptioninrtf <=> mirror.descriptioninrtf) = FALSE or (src.facttime <=> mirror.facttime) = FALSE or (src.impact_id <=> mirror.impact_id) = FALSE or (src.linkedsc <=> mirror.linkedsc) = FALSE or (src.location <=> mirror.location) = FALSE or (src.manualclosdate <=> mirror.manualclosdate) = FALSE or (src.mark <=> mirror.mark) = FALSE or (src.numreclass <=> mirror.numreclass) = FALSE or (src.numref <=> mirror.numref) = FALSE or (src.priority_id <=> mirror.priority_id) = FALSE or (src.proccodeclose <=> mirror.proccodeclose) = FALSE or (src.registration_date <=> mirror.registration_date) = FALSE or (src.resolutiontime <=> mirror.resolutiontime) = FALSE or (src.responsibleemployee_id <=> mirror.responsibleemployee_id) = FALSE or (src.responsibleteam_id <=> mirror.responsibleteam_id) = FALSE or (src.resumptionsum <=> mirror.resumptionsum) = FALSE or (src.route <=> mirror.route) = FALSE or (src.servicetime_id <=> mirror.servicetime_id) = FALSE or (src.sevicecomp <=> mirror.sevicecomp) = FALSE or (src.slastop <=> mirror.slastop) = FALSE or (src.slmservice <=> mirror.slmservice) = FALSE or (src.solution <=> mirror.solution) = FALSE or (src.solvedbyemployee_id <=> mirror.solvedbyemployee_id) = FALSE or (src.solvedbyteam_id <=> mirror.solvedbyteam_id) = FALSE or (src.state <=> mirror.state) = FALSE or (src.statestarttime <=> mirror.statestarttime) = FALSE or (src.stdonechildsc <=> mirror.stdonechildsc) = FALSE or (src.techdesc <=> mirror.techdesc) = FALSE or (src.timeclosing24 <=> mirror.timeclosing24) = FALSE or (src.timezone_id <=> mirror.timezone_id) = FALSE or (src.title <=> mirror.title) = FALSE or (src.urgency_id <=> mirror.urgency_id) = FALSE or (src.usercalls_reaction1line_l <=> mirror.usercalls_reaction1line_l) = FALSE or (src.usercalls_reaction2line_l <=> mirror.usercalls_reaction2line_l) = FALSE or (src.usercalls_reaction3line_l <=> mirror.usercalls_reaction3line_l) = FALSE or (src.usercalls_reaction4line_l <=> mirror.usercalls_reaction4line_l) = FALSE or (src.usercalls_timetorectific_l <=> mirror.usercalls_timetorectific_l) = FALSE or (src.usercalls_timework1line_l <=> mirror.usercalls_timework1line_l) = FALSE or (src.usercalls_timework2line_l <=> mirror.usercalls_timework2line_l) = FALSE or (src.usercalls_timework3line_l <=> mirror.usercalls_timework3line_l) = FALSE or (src.usercalls_timework4line_l <=> mirror.usercalls_timework4line_l) = FALSE or (src.username <=> mirror.username) = FALSE or (src.wayaddressing <=> mirror.wayaddressing) = FALSE or (src.whoclose <=> mirror.whoclose) = FALSE or (src.whodocontr_em <=> mirror.whodocontr_em) = FALSE or (src.overduestate_s <=> mirror.overduestate_s) = FALSE or (src.overduestate_d <=> mirror.overduestate_d) = FALSE)
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
                             THEN src.ci
                         WHEN (mirror.id is not null)
                             THEN mirror.ci
                         ELSE null
                     END AS ci
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.client_email
                         WHEN (mirror.id is not null)
                             THEN mirror.client_email
                         ELSE null
                     END AS client_email
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.client_name
                         WHEN (mirror.id is not null)
                             THEN mirror.client_name
                         ELSE null
                     END AS client_name
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.clientou_id
                         WHEN (mirror.id is not null)
                             THEN mirror.clientou_id
                         ELSE null
                     END AS clientou_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.datedecision
                         WHEN (mirror.id is not null)
                             THEN mirror.datedecision
                         ELSE null
                     END AS datedecision
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.deadLineDate_d
                         WHEN (mirror.id is not null)
                             THEN mirror.deadLineDate_d
                         ELSE null
                     END AS deadLineDate_d
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.deadlinetime
                         WHEN (mirror.id is not null)
                             THEN mirror.deadlinetime
                         ELSE null
                     END AS deadlinetime
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.descriptioninrtf
                         WHEN (mirror.id is not null)
                             THEN mirror.descriptioninrtf
                         ELSE null
                     END AS descriptioninrtf
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.facttime
                         WHEN (mirror.id is not null)
                             THEN mirror.facttime
                         ELSE null
                     END AS facttime
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.impact_id
                         WHEN (mirror.id is not null)
                             THEN mirror.impact_id
                         ELSE null
                     END AS impact_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.linkedsc
                         WHEN (mirror.id is not null)
                             THEN mirror.linkedsc
                         ELSE null
                     END AS linkedsc
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
                             THEN src.manualclosdate
                         WHEN (mirror.id is not null)
                             THEN mirror.manualclosdate
                         ELSE null
                     END AS manualclosdate
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.mark
                         WHEN (mirror.id is not null)
                             THEN mirror.mark
                         ELSE null
                     END AS mark
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.numreclass
                         WHEN (mirror.id is not null)
                             THEN mirror.numreclass
                         ELSE null
                     END AS numreclass
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.numref
                         WHEN (mirror.id is not null)
                             THEN mirror.numref
                         ELSE null
                     END AS numref
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.priority_id
                         WHEN (mirror.id is not null)
                             THEN mirror.priority_id
                         ELSE null
                     END AS priority_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.proccodeclose
                         WHEN (mirror.id is not null)
                             THEN mirror.proccodeclose
                         ELSE null
                     END AS proccodeclose
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.registration_date
                         WHEN (mirror.id is not null)
                             THEN mirror.registration_date
                         ELSE null
                     END AS registration_date
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.resolutiontime
                         WHEN (mirror.id is not null)
                             THEN mirror.resolutiontime
                         ELSE null
                     END AS resolutiontime
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
                             THEN src.resumptionsum
                         WHEN (mirror.id is not null)
                             THEN mirror.resumptionsum
                         ELSE null
                     END AS resumptionsum
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.route
                         WHEN (mirror.id is not null)
                             THEN mirror.route
                         ELSE null
                     END AS route
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.servicetime_id
                         WHEN (mirror.id is not null)
                             THEN mirror.servicetime_id
                         ELSE null
                     END AS servicetime_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.sevicecomp
                         WHEN (mirror.id is not null)
                             THEN mirror.sevicecomp
                         ELSE null
                     END AS sevicecomp
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.slastop
                         WHEN (mirror.id is not null)
                             THEN mirror.slastop
                         ELSE null
                     END AS slastop
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.slmservice
                         WHEN (mirror.id is not null)
                             THEN mirror.slmservice
                         ELSE null
                     END AS slmservice
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.solution
                         WHEN (mirror.id is not null)
                             THEN mirror.solution
                         ELSE null
                     END AS solution
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.solvedbyemployee_id
                         WHEN (mirror.id is not null)
                             THEN mirror.solvedbyemployee_id
                         ELSE null
                     END AS solvedbyemployee_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.solvedbyteam_id
                         WHEN (mirror.id is not null)
                             THEN mirror.solvedbyteam_id
                         ELSE null
                     END AS solvedbyteam_id
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
                             THEN src.stdonechildsc
                         WHEN (mirror.id is not null)
                             THEN mirror.stdonechildsc
                         ELSE null
                     END AS stdonechildsc
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.techdesc
                         WHEN (mirror.id is not null)
                             THEN mirror.techdesc
                         ELSE null
                     END AS techdesc
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.timeclosing24
                         WHEN (mirror.id is not null)
                             THEN mirror.timeclosing24
                         ELSE null
                     END AS timeclosing24
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.timezone_id
                         WHEN (mirror.id is not null)
                             THEN mirror.timezone_id
                         ELSE null
                     END AS timezone_id
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
                             THEN src.urgency_id
                         WHEN (mirror.id is not null)
                             THEN mirror.urgency_id
                         ELSE null
                     END AS urgency_id
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.usercalls_reaction1line_l
                         WHEN (mirror.id is not null)
                             THEN mirror.usercalls_reaction1line_l
                         ELSE null
                     END AS usercalls_reaction1line_l
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.usercalls_reaction2line_l
                         WHEN (mirror.id is not null)
                             THEN mirror.usercalls_reaction2line_l
                         ELSE null
                     END AS usercalls_reaction2line_l
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.usercalls_reaction3line_l
                         WHEN (mirror.id is not null)
                             THEN mirror.usercalls_reaction3line_l
                         ELSE null
                     END AS usercalls_reaction3line_l
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.usercalls_reaction4line_l
                         WHEN (mirror.id is not null)
                             THEN mirror.usercalls_reaction4line_l
                         ELSE null
                     END AS usercalls_reaction4line_l
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.usercalls_timetorectific_l
                         WHEN (mirror.id is not null)
                             THEN mirror.usercalls_timetorectific_l
                         ELSE null
                     END AS usercalls_timetorectific_l
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.usercalls_timework1line_l
                         WHEN (mirror.id is not null)
                             THEN mirror.usercalls_timework1line_l
                         ELSE null
                     END AS usercalls_timework1line_l
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.usercalls_timework2line_l
                         WHEN (mirror.id is not null)
                             THEN mirror.usercalls_timework2line_l
                         ELSE null
                     END AS usercalls_timework2line_l
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.usercalls_timework3line_l
                         WHEN (mirror.id is not null)
                             THEN mirror.usercalls_timework3line_l
                         ELSE null
                     END AS usercalls_timework3line_l
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.usercalls_timework4line_l
                         WHEN (mirror.id is not null)
                             THEN mirror.usercalls_timework4line_l
                         ELSE null
                     END AS usercalls_timework4line_l
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.username
                         WHEN (mirror.id is not null)
                             THEN mirror.username
                         ELSE null
                     END AS username
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.wayaddressing
                         WHEN (mirror.id is not null)
                             THEN mirror.wayaddressing
                         ELSE null
                     END AS wayaddressing
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.whoclose
                         WHEN (mirror.id is not null)
                             THEN mirror.whoclose
                         ELSE null
                     END AS whoclose
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.whodocontr_em
                         WHEN (mirror.id is not null)
                             THEN mirror.whodocontr_em
                         ELSE null
                     END AS whodocontr_em
        ,
                     CASE
                         WHEN (src.id is not null)
                             THEN src.overduestate_s
                         WHEN (mirror.id is not null)
                             THEN mirror.overduestate_s
                         ELSE null
                     END AS overduestate_s
        ,
                     CASE
                         WHEN (src.id is not null)
                             THEN src.overduestate_d
                         WHEN (mirror.id is not null)
                             THEN mirror.overduestate_d
                         ELSE null
                     END AS overduestate_d
        
                      FROM ${ods_schema_name}.tbl_servicecall_src AS src
                      FULL OUTER JOIN ${ods_schema_name}.tbl_servicecall 
                      AS mirror ON (src.id = mirror.id)) delta WHERE delta.dws_act <> "") delta_num ) rsl

                INSERT INTO TABLE ${ods_schema_name}.tbl_servicecall_delta partition (`date`)
                    SELECT dws_job, dws_act,
                    id, creation_date, last_modified_date, removal_date, removed, author_id, case_id, ci, client_email, client_name, clientou_id, datedecision, deadLineDate_d, deadlinetime, descriptioninrtf, facttime, impact_id, linkedsc, location, manualclosdate, mark, numreclass, numref, priority_id, proccodeclose, registration_date, resolutiontime, responsibleemployee_id, responsibleteam_id, resumptionsum, route, servicetime_id, sevicecomp, slastop, slmservice, solution, solvedbyemployee_id, solvedbyteam_id, state, statestarttime, stdonechildsc, techdesc, timeclosing24, timezone_id, title, urgency_id, usercalls_reaction1line_l, usercalls_reaction2line_l, usercalls_reaction3line_l, usercalls_reaction4line_l, usercalls_timetorectific_l, usercalls_timework1line_l, usercalls_timework2line_l, usercalls_timework3line_l, usercalls_timework4line_l, username, wayaddressing, whoclose, whodocontr_em, insert_date, overduestate_s, overduestate_d,
                    '${date}' as `date`
                WHERE row_num = 1

                INSERT INTO TABLE ${ods_schema_name}.tbl_servicecall_double
                    SELECT dws_job,
                    insert_date as as_of_date,
                    CASE 
                        WHEN (row_num = 1) THEN 'Y'
                    ELSE 'N' END AS effective_flag,
                    id, creation_date, last_modified_date, removal_date, removed, author_id, case_id, ci, client_email, client_name, clientou_id, datedecision, deadLineDate_d, deadlinetime, descriptioninrtf, facttime, impact_id, linkedsc, location, manualclosdate, mark, numreclass, numref, priority_id, proccodeclose, registration_date, resolutiontime, responsibleemployee_id, responsibleteam_id, resumptionsum, route, servicetime_id, sevicecomp, slastop, slmservice, solution, solvedbyemployee_id, solvedbyteam_id, state, statestarttime, stdonechildsc, techdesc, timeclosing24, timezone_id, title, urgency_id, usercalls_reaction1line_l, usercalls_reaction2line_l, usercalls_reaction3line_l, usercalls_reaction4line_l, usercalls_timetorectific_l, usercalls_timework1line_l, usercalls_timework2line_l, usercalls_timework3line_l, usercalls_timework4line_l, username, wayaddressing, whoclose, whodocontr_em, overduestate_s, overduestate_d
                    WHERE count_num > 1;
