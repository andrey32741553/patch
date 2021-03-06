set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_catalogs_delta
(
    dws_job                     string,
    dws_act                     string,
    nk                          string,
    dws_uniact                  string,
    id                          bigint,
    creation_date               string,
    last_modified_date          string,
    removal_date                string,
    removed                     boolean,
    title                       string,
    case_id                     string,
    num_                        bigint,
    idholder                    string,
    shorttitle                  string,
    holiday_startdate           string,
    holiday_parentops           bigint,
    holiday_datenotify          string,
    holiday_enddate             string,
    cimodel_vendor              bigint,
    route_templatedescr         string,
    route_cancheckci            boolean,
    workinghours_parentholiday  bigint,
    workinghours_parentops      bigint,
    workinghours_endworktimeh   bigint,
    workinghours_beginworktimem bigint,
    workinghours_beginworktimeh bigint,
    workinghours_weekday        bigint,
    workinghours_endworktimem   bigint,
    rfccodeclosing_rfpmaping    bigint,
    lunchtime_parentwt          bigint,
    author_id                   bigint,
    system_icon_id              bigint,
    route_impact                bigint,
    inccodeclosing_globalcodes  bigint,
    workresult_notsendconfirm   boolean,
    route_cancheckmail          boolean,
    route_canchecktel           boolean,
    state                       string,
    statestarttime              string,
    workinghours_beginplacework string,
    workinghours_endplacework   string,
    stages_process              bigint,
    code                        string,
    route_mandatapproval        boolean,
    route_negotstaddtkt         boolean,
    descr                       string,
    searchtitle                 string,
    route_grpallow              boolean,
    route_locallow              boolean,
    route_grpdesc               string,
    route_rtfallow              boolean,
    route_flallow               boolean,
    route_ciallow               boolean,
    vendor_source               bigint,
    integration_baseurl         string,
    integration_defaultroute    bigint,
    integration_routeexchange   bigint,
    integration_routeconnect    bigint,
    integration_blackoutdeepsc  bigint,
    integration_blackouttime    bigint,
    integration_accesskey       string,
    integration_servicecomexch  bigint,
    integration_servicecomconn  bigint,
    integration_ouext           bigint,
    integration_defaultservice  bigint,
    integration_slmserviceexch  bigint,
    integration_slmserviceconn  bigint,
    integration_reclclosecode   bigint,
    workresult_manualtimeclos   boolean,
    insert_date                 string
)
    PARTITIONED BY (`date` string)
    STORED AS ORC;

INSERT INTO TABLE ${ods_schema_name}.tbl_catalogs_delta partition (`date`)
(dws_job,
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
 idholder,
 shorttitle,
 holiday_startdate,
 holiday_parentops,
 holiday_datenotify,
 holiday_enddate,
 cimodel_vendor,
 route_templatedescr,
 route_cancheckci,
 workinghours_parentholiday,
 workinghours_parentops,
 workinghours_endworktimeh,
 workinghours_beginworktimem,
 workinghours_beginworktimeh,
 workinghours_weekday,
 workinghours_endworktimem,
 rfccodeclosing_rfpmaping,
 lunchtime_parentwt,
 author_id,
 system_icon_id,
 route_impact,
 inccodeclosing_globalcodes,
 workresult_notsendconfirm,
 route_cancheckmail,
 route_canchecktel,
 state,
 statestarttime,
 workinghours_beginplacework,
 workinghours_endplacework,
 stages_process,
 code,
 route_mandatapproval,
 route_negotstaddtkt,
 descr,
 searchtitle,
 route_grpallow,
 route_locallow,
 route_grpdesc,
 route_rtfallow,
 route_flallow,
 route_ciallow,
 vendor_source,
 integration_baseurl,
 integration_defaultroute,
 integration_routeexchange,
 integration_routeconnect,
 integration_blackoutdeepsc,
 integration_blackouttime,
 integration_accesskey,
 integration_servicecomexch,
 integration_servicecomconn,
 integration_ouext,
 integration_defaultservice,
 integration_slmserviceexch,
 integration_slmserviceconn,
 integration_reclclosecode,
 workresult_manualtimeclos,
 insert_date,
 `date`)
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
       idholder,
       shorttitle,
       holiday_startdate,
       holiday_parentops,
       holiday_datenotify,
       holiday_enddate,
       cimodel_vendor,
       route_templatedescr,
       route_cancheckci,
       workinghours_parentholiday,
       workinghours_parentops,
       workinghours_endworktimeh,
       workinghours_beginworktimem,
       workinghours_beginworktimeh,
       workinghours_weekday,
       workinghours_endworktimem,
       rfccodeclosing_rfpmaping,
       lunchtime_parentwt,
       author_id,
       system_icon_id,
       route_impact,
       inccodeclosing_globalcodes,
       workresult_notsendconfirm,
       route_cancheckmail,
       route_canchecktel,
       state,
       statestarttime,
       workinghours_beginplacework,
       workinghours_endplacework,
       stages_process,
       code,
       route_mandatapproval,
       route_negotstaddtkt,
       descr,
       searchtitle,
       route_grpallow,
       route_locallow,
       route_grpdesc,
       route_rtfallow,
       route_flallow,
       route_ciallow,
       vendor_source,
       integration_baseurl,
       integration_defaultroute,
       integration_routeexchange,
       integration_routeconnect,
       integration_blackoutdeepsc,
       integration_blackouttime,
       integration_accesskey,
       integration_servicecomexch,
       integration_servicecomconn,
       integration_ouext,
       integration_defaultservice,
       integration_slmserviceexch,
       integration_slmserviceconn,
       integration_reclclosecode,
       workresult_manualtimeclos,
       insert_date,
       `date`
FROM ${ods_schema_name}.tbl_catalogs_delta_tmp;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_catalogs_delta_tmp;