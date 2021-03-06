set hive.exec.dynamic.partition.mode=nonstrict;

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
    connectoo                 bigint,
    insert_date               string
)
    PARTITIONED BY (`date` string)
    STORED AS ORC;

INSERT INTO TABLE ${ods_schema_name}.tbl_location_delta partition (`date`)
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
 responsiblestarttime,
 state,
 statestarttime,
 parent,
 address,
 postoffice_typecode,
 postoffice_postalcodetype,
 postoffice_temporaryworkd,
 postoffice_priorityoo,
 postoffice_servzone,
 postoffice_lon,
 postoffice_temporaryworkr,
 postoffice_typename,
 postoffice_lat,
 postoffice_postid,
 postoffice_postalcodeptn,
 postoffice_postcode,
 postoffice_temporaryclosr,
 postoffice_shortname,
 postoffice_servicetime,
 postoffice_postalcodetc,
 postoffice_description,
 postoffice_privatecategor,
 postoffice_postalclass,
 postoffice_nearestofficep,
 postoffice_posttypeid,
 postoffice_temporaryclosd,
 postoffice_parentops,
 postoffice_istemporaryclo,
 postoffice_isclosed,
 postoffice_exactprecision,
 author_id,
 system_icon_id,
 responsibleemployee_id,
 responsibleteam_id,
 timezone,
 idholder,
 postoffice_location,
 connectoo,
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
       responsiblestarttime,
       state,
       statestarttime,
       parent,
       address,
       postoffice_typecode,
       postoffice_postalcodetype,
       postoffice_temporaryworkd,
       postoffice_priorityoo,
       postoffice_servzone,
       postoffice_lon,
       postoffice_temporaryworkr,
       postoffice_typename,
       postoffice_lat,
       postoffice_postid,
       postoffice_postalcodeptn,
       postoffice_postcode,
       postoffice_temporaryclosr,
       postoffice_shortname,
       postoffice_servicetime,
       postoffice_postalcodetc,
       postoffice_description,
       postoffice_privatecategor,
       postoffice_postalclass,
       postoffice_nearestofficep,
       postoffice_posttypeid,
       postoffice_temporaryclosd,
       postoffice_parentops,
       postoffice_istemporaryclo,
       postoffice_isclosed,
       postoffice_exactprecision,
       author_id,
       system_icon_id,
       responsibleemployee_id,
       responsibleteam_id,
       timezone,
       idholder,
       postoffice_location,
       connectoo,
       insert_date,
       `date`
FROM ${ods_schema_name}.tbl_location_delta_tmp;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_location_delta_tmp;