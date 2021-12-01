FROM (SELECT *, 
                      count(id) over (partition by id order by insert_date desc) as count_num,
                      row_number() over (partition by id order by insert_date desc) as row_num FROM
                (SELECT 
                    dws_job, insert_date, dws_act, nk, dws_uniact,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, num_, idholder, shorttitle, holiday_startdate, holiday_parentops, holiday_datenotify, holiday_enddate, cimodel_vendor, route_templatedescr, route_cancheckci, workinghours_parentholiday, workinghours_parentops, workinghours_endworktimeh, workinghours_beginworktimem, workinghours_beginworktimeh, workinghours_weekday, workinghours_endworktimem, rfccodeclosing_rfpmaping, lunchtime_parentwt, author_id, system_icon_id, route_impact, inccodeclosing_globalcodes, workresult_notsendconfirm, route_cancheckmail, route_canchecktel, state, statestarttime, workinghours_beginplacework, workinghours_endplacework, stages_process, code, route_mandatapproval, route_negotstaddtkt, descr, searchtitle, route_grpallow, route_locallow, route_grpdesc, route_rtfallow, route_flallow, route_ciallow, vendor_source, integration_baseurl, integration_defaultroute, integration_routeexchange, integration_routeconnect, integration_blackoutdeepsc, integration_blackouttime, integration_accesskey, integration_servicecomexch, integration_servicecomconn, integration_ouext, integration_defaultservice, integration_slmserviceexch, integration_slmserviceconn, integration_reclclosecode, workresult_manualtimeclos
                FROM (SELECT coalesce(mirror.nk, reflect("java.util.UUID", "randomUUID")) AS nk,
                             '${dws_job}'                                AS dws_job,
                             '${insert_date}'                              AS insert_date,
                             'N'                                     AS dws_uniact,
                             CASE
                                 WHEN ((mirror.id is null) or (mirror.id = src.id and mirror.deleted_flag = 'Y' and '${insert_date}' > mirror.insert_date))
                                     THEN 'I'
                                 WHEN (src.id is null and mirror.deleted_flag = 'N')
                                     THEN 'D'
                                 WHEN (mirror.deleted_flag = 'N') and ((src.creation_date <=> mirror.creation_date) = FALSE or (src.last_modified_date <=> mirror.last_modified_date) = FALSE or (src.removal_date <=> mirror.removal_date) = FALSE or (src.removed <=> mirror.removed) = FALSE or (src.title <=> mirror.title) = FALSE or (src.case_id <=> mirror.case_id) = FALSE or (src.num_ <=> mirror.num_) = FALSE or (src.idholder <=> mirror.idholder) = FALSE or (src.shorttitle <=> mirror.shorttitle) = FALSE or (src.holiday_startdate <=> mirror.holiday_startdate) = FALSE or (src.holiday_parentops <=> mirror.holiday_parentops) = FALSE or (src.holiday_datenotify <=> mirror.holiday_datenotify) = FALSE or (src.holiday_enddate <=> mirror.holiday_enddate) = FALSE or (src.cimodel_vendor <=> mirror.cimodel_vendor) = FALSE or (src.route_templatedescr <=> mirror.route_templatedescr) = FALSE or (src.route_cancheckci <=> mirror.route_cancheckci) = FALSE or (src.workinghours_parentholiday <=> mirror.workinghours_parentholiday) = FALSE or (src.workinghours_parentops <=> mirror.workinghours_parentops) = FALSE or (src.workinghours_endworktimeh <=> mirror.workinghours_endworktimeh) = FALSE or (src.workinghours_beginworktimem <=> mirror.workinghours_beginworktimem) = FALSE or (src.workinghours_beginworktimeh <=> mirror.workinghours_beginworktimeh) = FALSE or (src.workinghours_weekday <=> mirror.workinghours_weekday) = FALSE or (src.workinghours_endworktimem <=> mirror.workinghours_endworktimem) = FALSE or (src.rfccodeclosing_rfpmaping <=> mirror.rfccodeclosing_rfpmaping) = FALSE or (src.lunchtime_parentwt <=> mirror.lunchtime_parentwt) = FALSE or (src.author_id <=> mirror.author_id) = FALSE or (src.system_icon_id <=> mirror.system_icon_id) = FALSE or (src.route_impact <=> mirror.route_impact) = FALSE or (src.inccodeclosing_globalcodes <=> mirror.inccodeclosing_globalcodes) = FALSE or (src.workresult_notsendconfirm <=> mirror.workresult_notsendconfirm) = FALSE or (src.route_cancheckmail <=> mirror.route_cancheckmail) = FALSE or (src.route_canchecktel <=> mirror.route_canchecktel) = FALSE or (src.state <=> mirror.state) = FALSE or (src.statestarttime <=> mirror.statestarttime) = FALSE or (src.workinghours_beginplacework <=> mirror.workinghours_beginplacework) = FALSE or (src.workinghours_endplacework <=> mirror.workinghours_endplacework) = FALSE or (src.stages_process <=> mirror.stages_process) = FALSE or (src.code <=> mirror.code) = FALSE or (src.route_mandatapproval <=> mirror.route_mandatapproval) = FALSE or (src.route_negotstaddtkt <=> mirror.route_negotstaddtkt) = FALSE or (src.descr <=> mirror.descr) = FALSE or (src.searchtitle <=> mirror.searchtitle) = FALSE or (src.route_grpallow <=> mirror.route_grpallow) = FALSE or (src.route_locallow <=> mirror.route_locallow) = FALSE or (src.route_grpdesc <=> mirror.route_grpdesc) = FALSE or (src.route_rtfallow <=> mirror.route_rtfallow) = FALSE or (src.route_flallow <=> mirror.route_flallow) = FALSE or (src.route_ciallow <=> mirror.route_ciallow) = FALSE or (src.vendor_source <=> mirror.vendor_source) = FALSE or (src.integration_baseurl <=> mirror.integration_baseurl) = FALSE or (src.integration_defaultroute <=> mirror.integration_defaultroute) = FALSE or (src.integration_routeexchange <=> mirror.integration_routeexchange) = FALSE or (src.integration_routeconnect <=> mirror.integration_routeconnect) = FALSE or (src.integration_blackoutdeepsc <=> mirror.integration_blackoutdeepsc) = FALSE or (src.integration_blackouttime <=> mirror.integration_blackouttime) = FALSE or (src.integration_accesskey <=> mirror.integration_accesskey) = FALSE or (src.integration_servicecomexch <=> mirror.integration_servicecomexch) = FALSE or (src.integration_servicecomconn <=> mirror.integration_servicecomconn) = FALSE or (src.integration_ouext <=> mirror.integration_ouext) = FALSE or (src.integration_defaultservice <=> mirror.integration_defaultservice) = FALSE or (src.integration_slmserviceexch <=> mirror.integration_slmserviceexch) = FALSE or (src.integration_slmserviceconn <=> mirror.integration_slmserviceconn) = FALSE or (src.integration_reclclosecode <=> mirror.integration_reclclosecode) = FALSE or (src.workresult_manualtimeclos <=> mirror.workresult_manualtimeclos) = FALSE) 
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
                             THEN src.holiday_startdate
                         WHEN (mirror.id is not null)
                             THEN mirror.holiday_startdate
                         ELSE null
                     END AS holiday_startdate
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.holiday_parentops
                         WHEN (mirror.id is not null)
                             THEN mirror.holiday_parentops
                         ELSE null
                     END AS holiday_parentops
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.holiday_datenotify
                         WHEN (mirror.id is not null)
                             THEN mirror.holiday_datenotify
                         ELSE null
                     END AS holiday_datenotify
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.holiday_enddate
                         WHEN (mirror.id is not null)
                             THEN mirror.holiday_enddate
                         ELSE null
                     END AS holiday_enddate
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.cimodel_vendor
                         WHEN (mirror.id is not null)
                             THEN mirror.cimodel_vendor
                         ELSE null
                     END AS cimodel_vendor
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.route_templatedescr
                         WHEN (mirror.id is not null)
                             THEN mirror.route_templatedescr
                         ELSE null
                     END AS route_templatedescr
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.route_cancheckci
                         WHEN (mirror.id is not null)
                             THEN mirror.route_cancheckci
                         ELSE null
                     END AS route_cancheckci
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.workinghours_parentholiday
                         WHEN (mirror.id is not null)
                             THEN mirror.workinghours_parentholiday
                         ELSE null
                     END AS workinghours_parentholiday
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.workinghours_parentops
                         WHEN (mirror.id is not null)
                             THEN mirror.workinghours_parentops
                         ELSE null
                     END AS workinghours_parentops
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.workinghours_endworktimeh
                         WHEN (mirror.id is not null)
                             THEN mirror.workinghours_endworktimeh
                         ELSE null
                     END AS workinghours_endworktimeh
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.workinghours_beginworktimem
                         WHEN (mirror.id is not null)
                             THEN mirror.workinghours_beginworktimem
                         ELSE null
                     END AS workinghours_beginworktimem
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.workinghours_beginworktimeh
                         WHEN (mirror.id is not null)
                             THEN mirror.workinghours_beginworktimeh
                         ELSE null
                     END AS workinghours_beginworktimeh
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.workinghours_weekday
                         WHEN (mirror.id is not null)
                             THEN mirror.workinghours_weekday
                         ELSE null
                     END AS workinghours_weekday
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.workinghours_endworktimem
                         WHEN (mirror.id is not null)
                             THEN mirror.workinghours_endworktimem
                         ELSE null
                     END AS workinghours_endworktimem
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.rfccodeclosing_rfpmaping
                         WHEN (mirror.id is not null)
                             THEN mirror.rfccodeclosing_rfpmaping
                         ELSE null
                     END AS rfccodeclosing_rfpmaping
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.lunchtime_parentwt
                         WHEN (mirror.id is not null)
                             THEN mirror.lunchtime_parentwt
                         ELSE null
                     END AS lunchtime_parentwt
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
                             THEN src.route_impact
                         WHEN (mirror.id is not null)
                             THEN mirror.route_impact
                         ELSE null
                     END AS route_impact
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.inccodeclosing_globalcodes
                         WHEN (mirror.id is not null)
                             THEN mirror.inccodeclosing_globalcodes
                         ELSE null
                     END AS inccodeclosing_globalcodes
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.workresult_notsendconfirm
                         WHEN (mirror.id is not null)
                             THEN mirror.workresult_notsendconfirm
                         ELSE null
                     END AS workresult_notsendconfirm
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.route_cancheckmail
                         WHEN (mirror.id is not null)
                             THEN mirror.route_cancheckmail
                         ELSE null
                     END AS route_cancheckmail
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.route_canchecktel
                         WHEN (mirror.id is not null)
                             THEN mirror.route_canchecktel
                         ELSE null
                     END AS route_canchecktel
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
                             THEN src.workinghours_beginplacework
                         WHEN (mirror.id is not null)
                             THEN mirror.workinghours_beginplacework
                         ELSE null
                     END AS workinghours_beginplacework
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.workinghours_endplacework
                         WHEN (mirror.id is not null)
                             THEN mirror.workinghours_endplacework
                         ELSE null
                     END AS workinghours_endplacework
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.stages_process
                         WHEN (mirror.id is not null)
                             THEN mirror.stages_process
                         ELSE null
                     END AS stages_process
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
                             THEN src.route_mandatapproval
                         WHEN (mirror.id is not null)
                             THEN mirror.route_mandatapproval
                         ELSE null
                     END AS route_mandatapproval
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.route_negotstaddtkt
                         WHEN (mirror.id is not null)
                             THEN mirror.route_negotstaddtkt
                         ELSE null
                     END AS route_negotstaddtkt
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.descr
                         WHEN (mirror.id is not null)
                             THEN mirror.descr
                         ELSE null
                     END AS descr
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.searchtitle
                         WHEN (mirror.id is not null)
                             THEN mirror.searchtitle
                         ELSE null
                     END AS searchtitle
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.route_grpallow
                         WHEN (mirror.id is not null)
                             THEN mirror.route_grpallow
                         ELSE null
                     END AS route_grpallow
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.route_locallow
                         WHEN (mirror.id is not null)
                             THEN mirror.route_locallow
                         ELSE null
                     END AS route_locallow
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.route_grpdesc
                         WHEN (mirror.id is not null)
                             THEN mirror.route_grpdesc
                         ELSE null
                     END AS route_grpdesc
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.route_rtfallow
                         WHEN (mirror.id is not null)
                             THEN mirror.route_rtfallow
                         ELSE null
                     END AS route_rtfallow
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.route_flallow
                         WHEN (mirror.id is not null)
                             THEN mirror.route_flallow
                         ELSE null
                     END AS route_flallow
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.route_ciallow
                         WHEN (mirror.id is not null)
                             THEN mirror.route_ciallow
                         ELSE null
                     END AS route_ciallow
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.vendor_source
                         WHEN (mirror.id is not null)
                             THEN mirror.vendor_source
                         ELSE null
                     END AS vendor_source
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.integration_baseurl
                         WHEN (mirror.id is not null)
                             THEN mirror.integration_baseurl
                         ELSE null
                     END AS integration_baseurl
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.integration_defaultroute
                         WHEN (mirror.id is not null)
                             THEN mirror.integration_defaultroute
                         ELSE null
                     END AS integration_defaultroute
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.integration_routeexchange
                         WHEN (mirror.id is not null)
                             THEN mirror.integration_routeexchange
                         ELSE null
                     END AS integration_routeexchange
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.integration_routeconnect
                         WHEN (mirror.id is not null)
                             THEN mirror.integration_routeconnect
                         ELSE null
                     END AS integration_routeconnect
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.integration_blackoutdeepsc
                         WHEN (mirror.id is not null)
                             THEN mirror.integration_blackoutdeepsc
                         ELSE null
                     END AS integration_blackoutdeepsc
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.integration_blackouttime
                         WHEN (mirror.id is not null)
                             THEN mirror.integration_blackouttime
                         ELSE null
                     END AS integration_blackouttime
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.integration_accesskey
                         WHEN (mirror.id is not null)
                             THEN mirror.integration_accesskey
                         ELSE null
                     END AS integration_accesskey
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.integration_servicecomexch
                         WHEN (mirror.id is not null)
                             THEN mirror.integration_servicecomexch
                         ELSE null
                     END AS integration_servicecomexch
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.integration_servicecomconn
                         WHEN (mirror.id is not null)
                             THEN mirror.integration_servicecomconn
                         ELSE null
                     END AS integration_servicecomconn
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.integration_ouext
                         WHEN (mirror.id is not null)
                             THEN mirror.integration_ouext
                         ELSE null
                     END AS integration_ouext
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.integration_defaultservice
                         WHEN (mirror.id is not null)
                             THEN mirror.integration_defaultservice
                         ELSE null
                     END AS integration_defaultservice
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.integration_slmserviceexch
                         WHEN (mirror.id is not null)
                             THEN mirror.integration_slmserviceexch
                         ELSE null
                     END AS integration_slmserviceexch
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.integration_slmserviceconn
                         WHEN (mirror.id is not null)
                             THEN mirror.integration_slmserviceconn
                         ELSE null
                     END AS integration_slmserviceconn
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.integration_reclclosecode
                         WHEN (mirror.id is not null)
                             THEN mirror.integration_reclclosecode
                         ELSE null
                     END AS integration_reclclosecode
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.workresult_manualtimeclos
                         WHEN (mirror.id is not null)
                             THEN mirror.workresult_manualtimeclos
                         ELSE null
                     END AS workresult_manualtimeclos
        
                      FROM ${ods_schema_name}.tbl_catalogs_src AS src
                      FULL OUTER JOIN ${ods_schema_name}.tbl_catalogs 
                      AS mirror ON (src.id = mirror.id)) delta WHERE delta.dws_act <> "") delta_num ) rsl

                INSERT INTO TABLE ${ods_schema_name}.tbl_catalogs_delta partition (`date`)
                    SELECT dws_job, dws_act, nk, dws_uniact,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, num_, idholder, shorttitle, holiday_startdate, holiday_parentops, holiday_datenotify, holiday_enddate, cimodel_vendor, route_templatedescr, route_cancheckci, workinghours_parentholiday, workinghours_parentops, workinghours_endworktimeh, workinghours_beginworktimem, workinghours_beginworktimeh, workinghours_weekday, workinghours_endworktimem, rfccodeclosing_rfpmaping, lunchtime_parentwt, author_id, system_icon_id, route_impact, inccodeclosing_globalcodes, workresult_notsendconfirm, route_cancheckmail, route_canchecktel, state, statestarttime, workinghours_beginplacework, workinghours_endplacework, stages_process, code, route_mandatapproval, route_negotstaddtkt, descr, searchtitle, route_grpallow, route_locallow, route_grpdesc, route_rtfallow, route_flallow, route_ciallow, vendor_source, integration_baseurl, integration_defaultroute, integration_routeexchange, integration_routeconnect, integration_blackoutdeepsc, integration_blackouttime, integration_accesskey, integration_servicecomexch, integration_servicecomconn, integration_ouext, integration_defaultservice, integration_slmserviceexch, integration_slmserviceconn, integration_reclclosecode, workresult_manualtimeclos, 
                    insert_date, '${date}' as `date`
                WHERE row_num = 1

                INSERT INTO TABLE ${ods_schema_name}.tbl_catalogs_double
                    SELECT dws_job,
                    insert_date as as_of_date,
                    CASE 
                        WHEN (row_num = 1) THEN 'Y'
                    ELSE 'N' END AS effective_flag,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, num_, idholder, shorttitle, holiday_startdate, holiday_parentops, holiday_datenotify, holiday_enddate, cimodel_vendor, route_templatedescr, route_cancheckci, workinghours_parentholiday, workinghours_parentops, workinghours_endworktimeh, workinghours_beginworktimem, workinghours_beginworktimeh, workinghours_weekday, workinghours_endworktimem, rfccodeclosing_rfpmaping, lunchtime_parentwt, author_id, system_icon_id, route_impact, inccodeclosing_globalcodes, workresult_notsendconfirm, route_cancheckmail, route_canchecktel, state, statestarttime, workinghours_beginplacework, workinghours_endplacework, stages_process, code, route_mandatapproval, route_negotstaddtkt, descr, searchtitle, route_grpallow, route_locallow, route_grpdesc, route_rtfallow, route_flallow, route_ciallow, vendor_source, integration_baseurl, integration_defaultroute, integration_routeexchange, integration_routeconnect, integration_blackoutdeepsc, integration_blackouttime, integration_accesskey, integration_servicecomexch, integration_servicecomconn, integration_ouext, integration_defaultservice, integration_slmserviceexch, integration_slmserviceconn, integration_reclclosecode, workresult_manualtimeclos
                    WHERE count_num > 1;