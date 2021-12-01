CREATE SCHEMA IF NOT EXISTS ${ods_schema_name};

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_catalogs;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_catalogs_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_catalogs_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_catalogs_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_catalogs
(
    dws_job                     string,
    insert_date                 string,
    deleted_flag                string,
    nk                          string,
    default_flag                string,
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
    workresult_manualtimeclos   boolean
)
    STORED AS ORC;

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
    workresult_manualtimeclos   boolean
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_catalogs_src
(
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
    workresult_manualtimeclos   boolean
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_catalogs';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_catalogs_double
(
    dws_job                     string,
    as_of_date                  string,
    effective_flag              string,
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
    workresult_manualtimeclos   boolean
)
    STORED AS ORC;

CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_catalogs_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_catalogs;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_employee;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_employee_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_employee_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_employee_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_employee
(
    dws_job                     string,
    insert_date                 string,
    deleted_flag                string,
    nk                          string,
    default_flag                string,
    id                          bigint,
    creation_date               string,
    last_modified_date          string,
    removal_date                string,
    removed                     boolean,
    title                       string,
    case_id                     string,
    city_phone                  string,
    date_of_birth               string,
    email                       string,
    first_name                  string,
    home_phone                  string,
    internal_phone              string,
    last_name                   string,
    login                       string,
    middle_name                 string,
    mobile_phone                string,
    num_                        bigint,
    password                    string,
    post                        string,
    private_code                string,
    author_id                   bigint,
    system_icon_id              bigint,
    parent_id                   bigint,
    location                    bigint,
    timezone                    bigint,
    idholder                    string,
    headou                      bigint,
    iternum                     string,
    postelement                 bigint,
    dn                          string,
    is_employee_locked          boolean,
    password_change_date        string,
    password_must_be_changed    boolean,
    immheaduser                 bigint,
    lastlogindate               string,
    room                        string,
    intervalstart               string,
    intervalend                 string,
    comment_author_alias        string,
    is_employee_for_integration boolean
)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_employee_delta
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
    city_phone                  string,
    date_of_birth               string,
    email                       string,
    first_name                  string,
    home_phone                  string,
    internal_phone              string,
    last_name                   string,
    login                       string,
    middle_name                 string,
    mobile_phone                string,
    num_                        bigint,
    password                    string,
    post                        string,
    private_code                string,
    author_id                   bigint,
    system_icon_id              bigint,
    parent_id                   bigint,
    location                    bigint,
    timezone                    bigint,
    idholder                    string,
    headou                      bigint,
    iternum                     string,
    postelement                 bigint,
    dn                          string,
    is_employee_locked          boolean,
    password_change_date        string,
    password_must_be_changed    boolean,
    immheaduser                 bigint,
    lastlogindate               string,
    room                        string,
    intervalstart               string,
    intervalend                 string,
    comment_author_alias        string,
    is_employee_for_integration boolean
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_employee_src
(
    id                          bigint,
    creation_date               string,
    last_modified_date          string,
    removal_date                string,
    removed                     boolean,
    title                       string,
    case_id                     string,
    city_phone                  string,
    date_of_birth               string,
    email                       string,
    first_name                  string,
    home_phone                  string,
    internal_phone              string,
    last_name                   string,
    login                       string,
    middle_name                 string,
    mobile_phone                string,
    num_                        bigint,
    password                    string,
    post                        string,
    private_code                string,
    author_id                   bigint,
    system_icon_id              bigint,
    parent_id                   bigint,
    location                    bigint,
    timezone                    bigint,
    idholder                    string,
    headou                      bigint,
    iternum                     string,
    postelement                 bigint,
    dn                          string,
    is_employee_locked          boolean,
    password_change_date        string,
    password_must_be_changed    boolean,
    immheaduser                 bigint,
    lastlogindate               string,
    room                        string,
    intervalstart               string,
    intervalend                 string,
    comment_author_alias        string,
    is_employee_for_integration boolean
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_employee';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_employee_double
(
    dws_job                     string,
    as_of_date                  string,
    effective_flag              string,
    id                          bigint,
    creation_date               string,
    last_modified_date          string,
    removal_date                string,
    removed                     boolean,
    title                       string,
    case_id                     string,
    city_phone                  string,
    date_of_birth               string,
    email                       string,
    first_name                  string,
    home_phone                  string,
    internal_phone              string,
    last_name                   string,
    login                       string,
    middle_name                 string,
    mobile_phone                string,
    num_                        bigint,
    password                    string,
    post                        string,
    private_code                string,
    author_id                   bigint,
    system_icon_id              bigint,
    parent_id                   bigint,
    location                    bigint,
    timezone                    bigint,
    idholder                    string,
    headou                      bigint,
    iternum                     string,
    postelement                 bigint,
    dn                          string,
    is_employee_locked          boolean,
    password_change_date        string,
    password_must_be_changed    boolean,
    immheaduser                 bigint,
    lastlogindate               string,
    room                        string,
    intervalstart               string,
    intervalend                 string,
    comment_author_alias        string,
    is_employee_for_integration boolean
)
    STORED AS ORC;

CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_employee_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_employee;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_impact;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_impact_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_impact_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_impact_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_impact
(
    dws_job      string,
    insert_date  string,
    deleted_flag string,
    nk           string,
    default_flag string,
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_impact_delta
(
    dws_job      string,
    dws_act      string,
    nk           string,
    dws_uniact   string,
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_impact_src
(
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_impact';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_impact_double
(
    dws_job        string,
    as_of_date     string,
    effective_flag string,
    id             bigint,
    removal_date   string,
    removed        boolean,
    code           string,
    color          string,
    folder         boolean,
    pos            bigint,
    parent         bigint,
    title_en       string,
    title_client   string,
    title          string
)
    STORED AS ORC;

CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_impact_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_impact;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_location;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_location_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_location_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_location_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_location
(
    dws_job                   string,
    insert_date               string,
    deleted_flag              string,
    nk                        string,
    default_flag              string,
    id                        bigint,
    creation_date             string,
    last_modified_date        string,
    removal_date              string,
    removed                   boolean,
    title                     string,
    case_id                   string,
    num_                      bigint,
    responsiblestarttime      string,
    state                     string,
    statestarttime            string,
    parent                    bigint,
    address                   string,
    postoffice_typecode       string,
    postoffice_postalcodetype bigint,
    postoffice_temporaryworkd date,
    postoffice_priorityoo     bigint,
    postoffice_servzone       bigint,
    postoffice_lon            float,
    postoffice_temporaryworkr string,
    postoffice_typename       string,
    postoffice_lat            float,
    postoffice_postid         string,
    postoffice_postalcodeptn  string,
    postoffice_postcode       string,
    postoffice_temporaryclosr string,
    postoffice_shortname      string,
    postoffice_servicetime    bigint,
    postoffice_postalcodetc   string,
    postoffice_description    string,
    postoffice_privatecategor boolean,
    postoffice_postalclass    bigint,
    postoffice_nearestofficep string,
    postoffice_posttypeid     bigint,
    postoffice_temporaryclosd date,
    postoffice_parentops      bigint,
    postoffice_istemporaryclo boolean,
    postoffice_isclosed       boolean,
    postoffice_exactprecision boolean,
    author_id                 bigint,
    system_icon_id            bigint,
    responsibleemployee_id    bigint,
    responsibleteam_id        bigint,
    timezone                  bigint,
    idholder                  string,
    postoffice_location       bigint,
    connectoo                 bigint
)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_location_delta
(
    dws_job                   string,
    dws_act                   string,
    nk                        string,
    dws_uniact                string,
    id                        bigint,
    creation_date             string,
    last_modified_date        string,
    removal_date              string,
    removed                   boolean,
    title                     string,
    case_id                   string,
    num_                      bigint,
    responsiblestarttime      string,
    state                     string,
    statestarttime            string,
    parent                    bigint,
    address                   string,
    postoffice_typecode       string,
    postoffice_postalcodetype bigint,
    postoffice_temporaryworkd date,
    postoffice_priorityoo     bigint,
    postoffice_servzone       bigint,
    postoffice_lon            float,
    postoffice_temporaryworkr string,
    postoffice_typename       string,
    postoffice_lat            float,
    postoffice_postid         string,
    postoffice_postalcodeptn  string,
    postoffice_postcode       string,
    postoffice_temporaryclosr string,
    postoffice_shortname      string,
    postoffice_servicetime    bigint,
    postoffice_postalcodetc   string,
    postoffice_description    string,
    postoffice_privatecategor boolean,
    postoffice_postalclass    bigint,
    postoffice_nearestofficep string,
    postoffice_posttypeid     bigint,
    postoffice_temporaryclosd date,
    postoffice_parentops      bigint,
    postoffice_istemporaryclo boolean,
    postoffice_isclosed       boolean,
    postoffice_exactprecision boolean,
    author_id                 bigint,
    system_icon_id            bigint,
    responsibleemployee_id    bigint,
    responsibleteam_id        bigint,
    timezone                  bigint,
    idholder                  string,
    postoffice_location       bigint,
    connectoo                 bigint
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_location_src
(
    id                        bigint,
    creation_date             string,
    last_modified_date        string,
    removal_date              string,
    removed                   boolean,
    title                     string,
    case_id                   string,
    num_                      bigint,
    responsiblestarttime      string,
    state                     string,
    statestarttime            string,
    parent                    bigint,
    address                   string,
    postoffice_typecode       string,
    postoffice_postalcodetype bigint,
    postoffice_temporaryworkd date,
    postoffice_priorityoo     bigint,
    postoffice_servzone       bigint,
    postoffice_lon            float,
    postoffice_temporaryworkr string,
    postoffice_typename       string,
    postoffice_lat            float,
    postoffice_postid         string,
    postoffice_postalcodeptn  string,
    postoffice_postcode       string,
    postoffice_temporaryclosr string,
    postoffice_shortname      string,
    postoffice_servicetime    bigint,
    postoffice_postalcodetc   string,
    postoffice_description    string,
    postoffice_privatecategor boolean,
    postoffice_postalclass    bigint,
    postoffice_nearestofficep string,
    postoffice_posttypeid     bigint,
    postoffice_temporaryclosd date,
    postoffice_parentops      bigint,
    postoffice_istemporaryclo boolean,
    postoffice_isclosed       boolean,
    postoffice_exactprecision boolean,
    author_id                 bigint,
    system_icon_id            bigint,
    responsibleemployee_id    bigint,
    responsibleteam_id        bigint,
    timezone                  bigint,
    idholder                  string,
    postoffice_location       bigint,
    connectoo                 bigint
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_location';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_location_double
(
    dws_job                   string,
    as_of_date                string,
    effective_flag            string,
    id                        bigint,
    creation_date             string,
    last_modified_date        string,
    removal_date              string,
    removed                   boolean,
    title                     string,
    case_id                   string,
    num_                      bigint,
    responsiblestarttime      string,
    state                     string,
    statestarttime            string,
    parent                    bigint,
    address                   string,
    postoffice_typecode       string,
    postoffice_postalcodetype bigint,
    postoffice_temporaryworkd date,
    postoffice_priorityoo     bigint,
    postoffice_servzone       bigint,
    postoffice_lon            float,
    postoffice_temporaryworkr string,
    postoffice_typename       string,
    postoffice_lat            float,
    postoffice_postid         string,
    postoffice_postalcodeptn  string,
    postoffice_postcode       string,
    postoffice_temporaryclosr string,
    postoffice_shortname      string,
    postoffice_servicetime    bigint,
    postoffice_postalcodetc   string,
    postoffice_description    string,
    postoffice_privatecategor boolean,
    postoffice_postalclass    bigint,
    postoffice_nearestofficep string,
    postoffice_posttypeid     bigint,
    postoffice_temporaryclosd date,
    postoffice_parentops      bigint,
    postoffice_istemporaryclo boolean,
    postoffice_isclosed       boolean,
    postoffice_exactprecision boolean,
    author_id                 bigint,
    system_icon_id            bigint,
    responsibleemployee_id    bigint,
    responsibleteam_id        bigint,
    timezone                  bigint,
    idholder                  string,
    postoffice_location       bigint,
    connectoo                 bigint
)
    STORED AS ORC;

CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_location_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_location;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_mark;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_mark_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_mark_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_mark_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_mark
(
    dws_job      string,
    insert_date  string,
    deleted_flag string,
    nk           string,
    default_flag string,
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_mark_delta
(
    dws_job      string,
    dws_act      string,
    nk           string,
    dws_uniact   string,
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_mark_src
(
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_mark';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_mark_double
(
    dws_job        string,
    as_of_date     string,
    effective_flag string,
    id             bigint,
    removal_date   string,
    removed        boolean,
    code           string,
    color          string,
    folder         boolean,
    pos            bigint,
    parent         bigint,
    title_en       string,
    title_client   string,
    title          string
)
    STORED AS ORC;

CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_mark_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_mark;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_objectbase;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_objectbase_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_objectbase_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_objectbase_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_objectbase
(
    dws_job                  string,
    insert_date              string,
    deleted_flag             string,
    nk                       string,
    default_flag             string,
    id                       bigint,
    creation_date            string,
    last_modified_date       string,
    removal_date             string,
    removed                  boolean,
    archqueue                boolean,
    author_id                bigint,
    case_id                  string,
    cimodel                  bigint,
    configitem_activeaudit   bigint,
    configitem_existstatelog boolean,
    countcimass              bigint,
    datechcl                 string,
    datechrep                string,
    datefinishexp            string,
    dateofservprov           string,
    datestartexp             string,
    dnsname                  string,
    headplace                bigint,
    headslmservice           bigint,
    idholder                 string,
    invunit_monitoringdate   string,
    invunitqueue             bigint,
    ip                       string,
    lastauddate              string,
    lastcioffdate            string,
    lastclosedate            string,
    location                 bigint,
    mac                      string,
    num_                     bigint,
    parent                   bigint,
    prefix                   string,
    responsibleemployee_id   bigint,
    responsiblestarttime     string,
    responsibleteam_id       bigint,
    sn                       string,
    sourcecall               bigint,
    state                    string,
    statestarttime           string,
    supservice               bigint,
    supsrvtech               bigint,
    title                    string,
    vendor                   bigint
)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_objectbase_delta
(
    dws_job                  string,
    dws_act                  string,
    nk                       string,
    dws_uniact               string,
    id                       bigint,
    creation_date            string,
    last_modified_date       string,
    removal_date             string,
    removed                  boolean,
    archqueue                boolean,
    author_id                bigint,
    case_id                  string,
    cimodel                  bigint,
    configitem_activeaudit   bigint,
    configitem_existstatelog boolean,
    countcimass              bigint,
    datechcl                 string,
    datechrep                string,
    datefinishexp            string,
    dateofservprov           string,
    datestartexp             string,
    dnsname                  string,
    headplace                bigint,
    headslmservice           bigint,
    idholder                 string,
    invunit_monitoringdate   string,
    invunitqueue             bigint,
    ip                       string,
    lastauddate              string,
    lastcioffdate            string,
    lastclosedate            string,
    location                 bigint,
    mac                      string,
    num_                     bigint,
    parent                   bigint,
    prefix                   string,
    responsibleemployee_id   bigint,
    responsiblestarttime     string,
    responsibleteam_id       bigint,
    sn                       string,
    sourcecall               bigint,
    state                    string,
    statestarttime           string,
    supservice               bigint,
    supsrvtech               bigint,
    title                    string,
    vendor                   bigint
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_objectbase_src
(
    id                       bigint,
    creation_date            string,
    last_modified_date       string,
    removal_date             string,
    removed                  boolean,
    archqueue                boolean,
    author_id                bigint,
    case_id                  string,
    cimodel                  bigint,
    configitem_activeaudit   bigint,
    configitem_existstatelog boolean,
    countcimass              bigint,
    datechcl                 string,
    datechrep                string,
    datefinishexp            string,
    dateofservprov           string,
    datestartexp             string,
    dnsname                  string,
    headplace                bigint,
    headslmservice           bigint,
    idholder                 string,
    invunit_monitoringdate   string,
    invunitqueue             bigint,
    ip                       string,
    lastauddate              string,
    lastcioffdate            string,
    lastclosedate            string,
    location                 bigint,
    mac                      string,
    num_                     bigint,
    parent                   bigint,
    prefix                   string,
    responsibleemployee_id   bigint,
    responsiblestarttime     string,
    responsibleteam_id       bigint,
    sn                       string,
    sourcecall               bigint,
    state                    string,
    statestarttime           string,
    supservice               bigint,
    supsrvtech               bigint,
    title                    string,
    vendor                   bigint
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_objectbase';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_objectbase_double
(
    dws_job                  string,
    as_of_date               string,
    effective_flag           string,
    id                       bigint,
    creation_date            string,
    last_modified_date       string,
    removal_date             string,
    removed                  boolean,
    archqueue                boolean,
    author_id                bigint,
    case_id                  string,
    cimodel                  bigint,
    configitem_activeaudit   bigint,
    configitem_existstatelog boolean,
    countcimass              bigint,
    datechcl                 string,
    datechrep                string,
    datefinishexp            string,
    dateofservprov           string,
    datestartexp             string,
    dnsname                  string,
    headplace                bigint,
    headslmservice           bigint,
    idholder                 string,
    invunit_monitoringdate   string,
    invunitqueue             bigint,
    ip                       string,
    lastauddate              string,
    lastcioffdate            string,
    lastclosedate            string,
    location                 bigint,
    mac                      string,
    num_                     bigint,
    parent                   bigint,
    prefix                   string,
    responsibleemployee_id   bigint,
    responsiblestarttime     string,
    responsibleteam_id       bigint,
    sn                       string,
    sourcecall               bigint,
    state                    string,
    statestarttime           string,
    supservice               bigint,
    supsrvtech               bigint,
    title                    string,
    vendor                   bigint
)
    STORED AS ORC;

CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_objectbase_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_objectbase;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_ou;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_ou_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_ou_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_ou_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_ou
(
    dws_job            string,
    insert_date        string,
    deleted_flag       string,
    nk                 string,
    default_flag       string,
    id                 bigint,
    creation_date      string,
    last_modified_date string,
    removal_date       string,
    removed            boolean,
    title              string,
    case_id            string,
    num_               bigint,
    author_id          bigint,
    system_icon_id     bigint,
    head_id            bigint,
    parent_id          bigint,
    accesskey          string,
    namefromad         string,
    idholder           string,
    shorttitle         string,
    accesskeyintrl     bigint,
    ouext_teampartners bigint,
    location           bigint,
    updated            string,
    headou             bigint,
    ouext_syncuser     bigint
)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_ou_delta
(
    dws_job            string,
    dws_act            string,
    nk                 string,
    dws_uniact         string,
    id                 bigint,
    creation_date      string,
    last_modified_date string,
    removal_date       string,
    removed            boolean,
    title              string,
    case_id            string,
    num_               bigint,
    author_id          bigint,
    system_icon_id     bigint,
    head_id            bigint,
    parent_id          bigint,
    accesskey          string,
    namefromad         string,
    idholder           string,
    shorttitle         string,
    accesskeyintrl     bigint,
    ouext_teampartners bigint,
    location           bigint,
    updated            string,
    headou             bigint,
    ouext_syncuser     bigint
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_ou_src
(
    id                 bigint,
    creation_date      string,
    last_modified_date string,
    removal_date       string,
    removed            boolean,
    title              string,
    case_id            string,
    num_               bigint,
    author_id          bigint,
    system_icon_id     bigint,
    head_id            bigint,
    parent_id          bigint,
    accesskey          string,
    namefromad         string,
    idholder           string,
    shorttitle         string,
    accesskeyintrl     bigint,
    ouext_teampartners bigint,
    location           bigint,
    updated            string,
    headou             bigint,
    ouext_syncuser     bigint
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_ou';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_ou_double
(
    dws_job            string,
    as_of_date         string,
    effective_flag     string,
    id                 bigint,
    creation_date      string,
    last_modified_date string,
    removal_date       string,
    removed            boolean,
    title              string,
    case_id            string,
    num_               bigint,
    author_id          bigint,
    system_icon_id     bigint,
    head_id            bigint,
    parent_id          bigint,
    accesskey          string,
    namefromad         string,
    idholder           string,
    shorttitle         string,
    accesskeyintrl     bigint,
    ouext_teampartners bigint,
    location           bigint,
    updated            string,
    headou             bigint,
    ouext_syncuser     bigint
)
    STORED AS ORC;

CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_ou_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_ou;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_priority;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_priority_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_priority_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_priority_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_priority
(
    dws_job        string,
    insert_date    string,
    deleted_flag   string,
    nk             string,
    default_flag   string,
    id             bigint,
    removal_date   string,
    removed        boolean,
    code           string,
    color          string,
    folder         boolean,
    pos            bigint,
    priority_level bigint,
    parent         bigint,
    title_en       string,
    title_client   string,
    title          string
)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_priority_delta
(
    dws_job        string,
    dws_act        string,
    nk             string,
    dws_uniact     string,
    id             bigint,
    removal_date   string,
    removed        boolean,
    code           string,
    color          string,
    folder         boolean,
    pos            bigint,
    priority_level bigint,
    parent         bigint,
    title_en       string,
    title_client   string,
    title          string
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_priority_src
(
    id             bigint,
    removal_date   string,
    removed        boolean,
    code           string,
    color          string,
    folder         boolean,
    pos            bigint,
    priority_level bigint,
    parent         bigint,
    title_en       string,
    title_client   string,
    title          string
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_priority';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_priority_double
(
    dws_job        string,
    as_of_date     string,
    effective_flag string,
    id             bigint,
    removal_date   string,
    removed        boolean,
    code           string,
    color          string,
    folder         boolean,
    pos            bigint,
    priority_level bigint,
    parent         bigint,
    title_en       string,
    title_client   string,
    title          string
)
    STORED AS ORC;

CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_priority_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_priority;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_service;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_service_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_service_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_service_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_service
(
    dws_job                string,
    insert_date            string,
    deleted_flag           string,
    nk                     string,
    default_flag           string,
    id                     bigint,
    creation_date          string,
    last_modified_date     string,
    removal_date           string,
    removed                boolean,
    title                  string,
    case_id                string,
    num_                   bigint,
    responsiblestarttime   string,
    state                  string,
    statestarttime         string,
    inventorynum           string,
    description            string,
    groupexprfc            bigint,
    groupdevman            bigint,
    groupregrfcman         bigint,
    idholder               string,
    parent                 bigint,
    groupitservman         bigint,
    techservice_provider   bigint,
    author_id              bigint,
    system_icon_id         bigint,
    responsibleemployee_id bigint,
    responsibleteam_id     bigint,
    emailsc                string,
    compcheck              boolean,
    groupbisclient         bigint,
    groupresprfc           bigint,
    negotstaddtkt          boolean,
    defstart               boolean,
    resippolice            bigint,
    rtfallow               boolean,
    grpallow               boolean,
    locallow               boolean,
    grpdesc                string,
    flallow                boolean,
    ciallow                boolean,
    prefix                 string,
    canchecksn             boolean,
    monitoringlink         boolean,
    archiveaction          bigint,
    stabilmangroup         bigint,
    mangroupinv            bigint,
    assetpolicy            bigint,
    usericon               bigint,
    routcinotfound         bigint,
    groupregisprb          bigint,
    prbregispolicy         bigint
)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_service_delta
(
    dws_job                string,
    dws_act                string,
    nk                     string,
    dws_uniact             string,
    id                     bigint,
    creation_date          string,
    last_modified_date     string,
    removal_date           string,
    removed                boolean,
    title                  string,
    case_id                string,
    num_                   bigint,
    responsiblestarttime   string,
    state                  string,
    statestarttime         string,
    inventorynum           string,
    description            string,
    groupexprfc            bigint,
    groupdevman            bigint,
    groupregrfcman         bigint,
    idholder               string,
    parent                 bigint,
    groupitservman         bigint,
    techservice_provider   bigint,
    author_id              bigint,
    system_icon_id         bigint,
    responsibleemployee_id bigint,
    responsibleteam_id     bigint,
    emailsc                string,
    compcheck              boolean,
    groupbisclient         bigint,
    groupresprfc           bigint,
    negotstaddtkt          boolean,
    defstart               boolean,
    resippolice            bigint,
    rtfallow               boolean,
    grpallow               boolean,
    locallow               boolean,
    grpdesc                string,
    flallow                boolean,
    ciallow                boolean,
    prefix                 string,
    canchecksn             boolean,
    monitoringlink         boolean,
    archiveaction          bigint,
    stabilmangroup         bigint,
    mangroupinv            bigint,
    assetpolicy            bigint,
    usericon               bigint,
    routcinotfound         bigint,
    groupregisprb          bigint,
    prbregispolicy         bigint
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_service_src
(
    id                     bigint,
    creation_date          string,
    last_modified_date     string,
    removal_date           string,
    removed                boolean,
    title                  string,
    case_id                string,
    num_                   bigint,
    responsiblestarttime   string,
    state                  string,
    statestarttime         string,
    inventorynum           string,
    description            string,
    groupexprfc            bigint,
    groupdevman            bigint,
    groupregrfcman         bigint,
    idholder               string,
    parent                 bigint,
    groupitservman         bigint,
    techservice_provider   bigint,
    author_id              bigint,
    system_icon_id         bigint,
    responsibleemployee_id bigint,
    responsibleteam_id     bigint,
    emailsc                string,
    compcheck              boolean,
    groupbisclient         bigint,
    groupresprfc           bigint,
    negotstaddtkt          boolean,
    defstart               boolean,
    resippolice            bigint,
    rtfallow               boolean,
    grpallow               boolean,
    locallow               boolean,
    grpdesc                string,
    flallow                boolean,
    ciallow                boolean,
    prefix                 string,
    canchecksn             boolean,
    monitoringlink         boolean,
    archiveaction          bigint,
    stabilmangroup         bigint,
    mangroupinv            bigint,
    assetpolicy            bigint,
    usericon               bigint,
    routcinotfound         bigint,
    groupregisprb          bigint,
    prbregispolicy         bigint
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_service';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_service_double
(
    dws_job                string,
    as_of_date             string,
    effective_flag         string,
    id                     bigint,
    creation_date          string,
    last_modified_date     string,
    removal_date           string,
    removed                boolean,
    title                  string,
    case_id                string,
    num_                   bigint,
    responsiblestarttime   string,
    state                  string,
    statestarttime         string,
    inventorynum           string,
    description            string,
    groupexprfc            bigint,
    groupdevman            bigint,
    groupregrfcman         bigint,
    idholder               string,
    parent                 bigint,
    groupitservman         bigint,
    techservice_provider   bigint,
    author_id              bigint,
    system_icon_id         bigint,
    responsibleemployee_id bigint,
    responsibleteam_id     bigint,
    emailsc                string,
    compcheck              boolean,
    groupbisclient         bigint,
    groupresprfc           bigint,
    negotstaddtkt          boolean,
    defstart               boolean,
    resippolice            bigint,
    rtfallow               boolean,
    grpallow               boolean,
    locallow               boolean,
    grpdesc                string,
    flallow                boolean,
    ciallow                boolean,
    prefix                 string,
    canchecksn             boolean,
    monitoringlink         boolean,
    archiveaction          bigint,
    stabilmangroup         bigint,
    mangroupinv            bigint,
    assetpolicy            bigint,
    usericon               bigint,
    routcinotfound         bigint,
    groupregisprb          bigint,
    prbregispolicy         bigint
)
    STORED AS ORC;

CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_service_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_service;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_servicecall;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_servicecall_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_servicecall_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_servicecall_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_servicecall
(
    dws_job                    string,
    insert_date                string,
    deleted_flag               string,
    id                         bigint,
    creation_date              string,
    last_modified_date         string,
    removal_date               string,
    removed                    boolean,
    author_id                  bigint,
    case_id                    string,
    ci                         bigint,
    client_email               string,
    client_name                string,
    clientou_id                bigint,
    datedecision               string,
    deadLineDate_d             bigint,
    deadlinetime               string,
    descriptioninrtf           string,
    facttime                   bigint,
    impact_id                  bigint,
    linkedsc                   bigint,
    location                   bigint,
    manualclosdate             string,
    mark                       bigint,
    numreclass                 bigint,
    numref                     bigint,
    priority_id                bigint,
    proccodeclose              bigint,
    registration_date          string,
    resolutiontime             bigint,
    responsibleemployee_id     bigint,
    responsibleteam_id         bigint,
    resumptionsum              bigint,
    route                      bigint,
    servicetime_id             bigint,
    sevicecomp                 bigint,
    slastop                    boolean,
    slmservice                 bigint,
    solution                   string,
    solvedbyemployee_id        bigint,
    solvedbyteam_id            bigint,
    state                      string,
    statestarttime             string,
    stdonechildsc              bigint,
    techdesc                   string,
    timeclosing24              string,
    timezone_id                bigint,
    title                      string,
    urgency_id                 bigint,
    usercalls_reaction1line_l  bigint,
    usercalls_reaction2line_l  bigint,
    usercalls_reaction3line_l  bigint,
    usercalls_reaction4line_l  bigint,
    usercalls_timetorectific_l bigint,
    usercalls_timework1line_l  bigint,
    usercalls_timework2line_l  bigint,
    usercalls_timework3line_l  bigint,
    usercalls_timework4line_l  bigint,
    username                   bigint,
    wayaddressing              bigint,
    whoclose                   bigint,
    whodocontr_em              bigint
)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_servicecall_delta
(
    dws_job                    string,
    dws_act                    string,
    id                         bigint,
    creation_date              string,
    last_modified_date         string,
    removal_date               string,
    removed                    boolean,
    author_id                  bigint,
    case_id                    string,
    ci                         bigint,
    client_email               string,
    client_name                string,
    clientou_id                bigint,
    datedecision               string,
    deadLineDate_d             bigint,
    deadlinetime               string,
    descriptioninrtf           string,
    facttime                   bigint,
    impact_id                  bigint,
    linkedsc                   bigint,
    location                   bigint,
    manualclosdate             string,
    mark                       bigint,
    numreclass                 bigint,
    numref                     bigint,
    priority_id                bigint,
    proccodeclose              bigint,
    registration_date          string,
    resolutiontime             bigint,
    responsibleemployee_id     bigint,
    responsibleteam_id         bigint,
    resumptionsum              bigint,
    route                      bigint,
    servicetime_id             bigint,
    sevicecomp                 bigint,
    slastop                    boolean,
    slmservice                 bigint,
    solution                   string,
    solvedbyemployee_id        bigint,
    solvedbyteam_id            bigint,
    state                      string,
    statestarttime             string,
    stdonechildsc              bigint,
    techdesc                   string,
    timeclosing24              string,
    timezone_id                bigint,
    title                      string,
    urgency_id                 bigint,
    usercalls_reaction1line_l  bigint,
    usercalls_reaction2line_l  bigint,
    usercalls_reaction3line_l  bigint,
    usercalls_reaction4line_l  bigint,
    usercalls_timetorectific_l bigint,
    usercalls_timework1line_l  bigint,
    usercalls_timework2line_l  bigint,
    usercalls_timework3line_l  bigint,
    usercalls_timework4line_l  bigint,
    username                   bigint,
    wayaddressing              bigint,
    whoclose                   bigint,
    whodocontr_em              bigint
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_servicecall_src
(
    id                         bigint,
    creation_date              string,
    last_modified_date         string,
    removal_date               string,
    removed                    boolean,
    author_id                  bigint,
    case_id                    string,
    ci                         bigint,
    client_email               string,
    client_name                string,
    clientou_id                bigint,
    datedecision               string,
    deadLineDate_d             bigint,
    deadlinetime               string,
    descriptioninrtf           string,
    facttime                   bigint,
    impact_id                  bigint,
    linkedsc                   bigint,
    location                   bigint,
    manualclosdate             string,
    mark                       bigint,
    numreclass                 bigint,
    numref                     bigint,
    priority_id                bigint,
    proccodeclose              bigint,
    registration_date          string,
    resolutiontime             bigint,
    responsibleemployee_id     bigint,
    responsibleteam_id         bigint,
    resumptionsum              bigint,
    route                      bigint,
    servicetime_id             bigint,
    sevicecomp                 bigint,
    slastop                    boolean,
    slmservice                 bigint,
    solution                   string,
    solvedbyemployee_id        bigint,
    solvedbyteam_id            bigint,
    state                      string,
    statestarttime             string,
    stdonechildsc              bigint,
    techdesc                   string,
    timeclosing24              string,
    timezone_id                bigint,
    title                      string,
    urgency_id                 bigint,
    usercalls_reaction1line_l  bigint,
    usercalls_reaction2line_l  bigint,
    usercalls_reaction3line_l  bigint,
    usercalls_reaction4line_l  bigint,
    usercalls_timetorectific_l bigint,
    usercalls_timework1line_l  bigint,
    usercalls_timework2line_l  bigint,
    usercalls_timework3line_l  bigint,
    usercalls_timework4line_l  bigint,
    username                   bigint,
    wayaddressing              bigint,
    whoclose                   bigint,
    whodocontr_em              bigint
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_servicecall';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_servicecall_double
(
    dws_job                    string,
    as_of_date                 string,
    effective_flag             string,
    id                         bigint,
    creation_date              string,
    last_modified_date         string,
    removal_date               string,
    removed                    boolean,
    author_id                  bigint,
    case_id                    string,
    ci                         bigint,
    client_email               string,
    client_name                string,
    clientou_id                bigint,
    datedecision               string,
    deadLineDate_d             bigint,
    deadlinetime               string,
    descriptioninrtf           string,
    facttime                   bigint,
    impact_id                  bigint,
    linkedsc                   bigint,
    location                   bigint,
    manualclosdate             string,
    mark                       bigint,
    numreclass                 bigint,
    numref                     bigint,
    priority_id                bigint,
    proccodeclose              bigint,
    registration_date          string,
    resolutiontime             bigint,
    responsibleemployee_id     bigint,
    responsibleteam_id         bigint,
    resumptionsum              bigint,
    route                      bigint,
    servicetime_id             bigint,
    sevicecomp                 bigint,
    slastop                    boolean,
    slmservice                 bigint,
    solution                   string,
    solvedbyemployee_id        bigint,
    solvedbyteam_id            bigint,
    state                      string,
    statestarttime             string,
    stdonechildsc              bigint,
    techdesc                   string,
    timeclosing24              string,
    timezone_id                bigint,
    title                      string,
    urgency_id                 bigint,
    usercalls_reaction1line_l  bigint,
    usercalls_reaction2line_l  bigint,
    usercalls_reaction3line_l  bigint,
    usercalls_reaction4line_l  bigint,
    usercalls_timetorectific_l bigint,
    usercalls_timework1line_l  bigint,
    usercalls_timework2line_l  bigint,
    usercalls_timework3line_l  bigint,
    usercalls_timework4line_l  bigint,
    username                   bigint,
    wayaddressing              bigint,
    whoclose                   bigint,
    whodocontr_em              bigint
)
    STORED AS ORC;



DROP TABLE IF EXISTS ${ods_schema_name}.tbl_servicetime;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_servicetime_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_servicetime_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_servicetime_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_servicetime
(
    dws_job       string,
    insert_date   string,
    deleted_flag  string,
    nk            string,
    default_flag  string,
    id            bigint,
    removal_date  string,
    removed       boolean,
    description   string,
    status        string,
    activecopy_id bigint,
    code          string,
    color         string,
    folder        boolean,
    pos           bigint,
    parent        bigint,
    title_en      string,
    title_client  string,
    title         string
)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_servicetime_delta
(
    dws_job       string,
    dws_act       string,
    nk            string,
    dws_uniact    string,
    id            bigint,
    removal_date  string,
    removed       boolean,
    description   string,
    status        string,
    activecopy_id bigint,
    code          string,
    color         string,
    folder        boolean,
    pos           bigint,
    parent        bigint,
    title_en      string,
    title_client  string,
    title         string
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_servicetime_src
(
    id            bigint,
    removal_date  string,
    removed       boolean,
    description   string,
    status        string,
    activecopy_id bigint,
    code          string,
    color         string,
    folder        boolean,
    pos           bigint,
    parent        bigint,
    title_en      string,
    title_client  string,
    title         string
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_servicetime';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_servicetime_double
(
    dws_job        string,
    as_of_date     string,
    effective_flag string,
    id             bigint,
    removal_date   string,
    removed        boolean,
    description    string,
    status         string,
    activecopy_id  bigint,
    code           string,
    color          string,
    folder         boolean,
    pos            bigint,
    parent         bigint,
    title_en       string,
    title_client   string,
    title          string
)
    STORED AS ORC;

CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_servicetime_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_servicetime;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_stdonechildsc;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_stdonechildsc_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_stdonechildsc_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_stdonechildsc_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_stdonechildsc
(
    dws_job      string,
    insert_date  string,
    deleted_flag string,
    nk           string,
    default_flag string,
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_stdonechildsc_delta
(
    dws_job      string,
    dws_act      string,
    nk           string,
    dws_uniact   string,
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_stdonechildsc_src
(
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_stdonechildsc';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_stdonechildsc_double
(
    dws_job        string,
    as_of_date     string,
    effective_flag string,
    id             bigint,
    removal_date   string,
    removed        boolean,
    code           string,
    color          string,
    folder         boolean,
    pos            bigint,
    parent         bigint,
    title_en       string,
    title_client   string,
    title          string
)
    STORED AS ORC;

CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_stdonechildsc_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_stdonechildsc;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_team;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_team_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_team_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_team_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_team
(
    dws_job            string,
    insert_date        string,
    deleted_flag       string,
    nk                 string,
    default_flag       string,
    id                 bigint,
    creation_date      string,
    last_modified_date string,
    removal_date       string,
    removed            boolean,
    case_id            string,
    num_               bigint,
    author_id          bigint,
    system_icon_id     bigint,
    headou_id          bigint,
    leader_id          bigint,
    teamname           string,
    description        string,
    grouprule          bigint,
    code               string,
    dutygroup          bigint,
    maingroup          bigint,
    headgroup          bigint,
    role_linktoprocess bigint,
    title              string
)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_team_delta
(
    dws_job            string,
    dws_act            string,
    nk                 string,
    dws_uniact         string,
    id                 bigint,
    creation_date      string,
    last_modified_date string,
    removal_date       string,
    removed            boolean,
    case_id            string,
    num_               bigint,
    author_id          bigint,
    system_icon_id     bigint,
    headou_id          bigint,
    leader_id          bigint,
    teamname           string,
    description        string,
    grouprule          bigint,
    code               string,
    dutygroup          bigint,
    maingroup          bigint,
    headgroup          bigint,
    role_linktoprocess bigint,
    title              string
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_team_src
(
    id                 bigint,
    creation_date      string,
    last_modified_date string,
    removal_date       string,
    removed            boolean,
    case_id            string,
    num_               bigint,
    author_id          bigint,
    system_icon_id     bigint,
    headou_id          bigint,
    leader_id          bigint,
    teamname           string,
    description        string,
    grouprule          bigint,
    code               string,
    dutygroup          bigint,
    maingroup          bigint,
    headgroup          bigint,
    role_linktoprocess bigint,
    title              string
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_team';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_team_double
(
    dws_job            string,
    as_of_date         string,
    effective_flag     string,
    id                 bigint,
    creation_date      string,
    last_modified_date string,
    removal_date       string,
    removed            boolean,
    case_id            string,
    num_               bigint,
    author_id          bigint,
    system_icon_id     bigint,
    headou_id          bigint,
    leader_id          bigint,
    teamname           string,
    description        string,
    grouprule          bigint,
    code               string,
    dutygroup          bigint,
    maingroup          bigint,
    headgroup          bigint,
    role_linktoprocess bigint,
    title              string
)
    STORED AS ORC;

CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_team_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_team;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_timezone;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_timezone_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_timezone_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_timezone_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_timezone
(
    dws_job      string,
    insert_date  string,
    deleted_flag string,
    nk           string,
    default_flag string,
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_timezone_delta
(
    dws_job      string,
    dws_act      string,
    nk           string,
    dws_uniact   string,
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_timezone_src
(
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_timezone';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_timezone_double
(
    dws_job        string,
    as_of_date     string,
    effective_flag string,
    id             bigint,
    removal_date   string,
    removed        boolean,
    code           string,
    color          string,
    folder         boolean,
    pos            bigint,
    parent         bigint,
    title_en       string,
    title_client   string,
    title          string
)
    STORED AS ORC;

CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_timezone_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_timezone;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_urgency;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_urgency_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_urgency_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_urgency_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_urgency
(
    dws_job      string,
    insert_date  string,
    deleted_flag string,
    nk           string,
    default_flag string,
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_urgency_delta
(
    dws_job      string,
    dws_act      string,
    nk           string,
    dws_uniact   string,
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_urgency_src
(
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_urgency';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_urgency_double
(
    dws_job        string,
    as_of_date     string,
    effective_flag string,
    id             bigint,
    removal_date   string,
    removed        boolean,
    code           string,
    color          string,
    folder         boolean,
    pos            bigint,
    parent         bigint,
    title_en       string,
    title_client   string,
    title          string
)
    STORED AS ORC;

CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_urgency_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_urgency;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_wayadressing;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_wayadressing_src;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_wayadressing_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_wayadressing_double;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_wayadressing
(
    dws_job      string,
    insert_date  string,
    deleted_flag string,
    nk           string,
    default_flag string,
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_wayadressing_delta
(
    dws_job      string,
    dws_act      string,
    nk           string,
    dws_uniact   string,
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
) PARTITIONED BY (insert_date string)
    STORED AS ORC;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_wayadressing_src
(
    id           bigint,
    removal_date string,
    removed      boolean,
    code         string,
    color        string,
    folder       boolean,
    pos          bigint,
    parent       bigint,
    title_en     string,
    title_client string,
    title        string
)
    STORED AS AVRO
    LOCATION '${hdfs_path}tbl_wayadressing';

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_wayadressing_double
(
    dws_job        string,
    as_of_date     string,
    effective_flag string,
    id             bigint,
    removal_date   string,
    removed        boolean,
    code           string,
    color          string,
    folder         boolean,
    pos            bigint,
    parent         bigint,
    title_en       string,
    title_client   string,
    title          string
)
    STORED AS ORC;

CREATE VIEW IF NOT EXISTS ${ods_schema_name}.tbl_wayadressing_nklink as
SELECT id as id,
       nk,
       dws_job,
       default_flag
FROM ${ods_schema_name}.tbl_wayadressing;

