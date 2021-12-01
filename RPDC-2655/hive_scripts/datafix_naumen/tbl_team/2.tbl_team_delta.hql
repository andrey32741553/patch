set hive.exec.dynamic.partition.mode=nonstrict;

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
    title              string,
    insert_date        string
)
    PARTITIONED BY (`date` string)
    STORED AS ORC;

INSERT INTO TABLE ${ods_schema_name}.tbl_team_delta partition (`date`)
(dws_job,
       dws_act,
       nk,
       dws_uniact,
       id,
       creation_date,
       last_modified_date,
       removal_date,
       removed,
       case_id,
       num_,
       author_id,
       system_icon_id,
       headou_id,
       leader_id,
       teamname,
       description,
       grouprule,
       code,
       dutygroup,
       maingroup,
       headgroup,
       role_linktoprocess,
       title,
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
       case_id,
       num_,
       author_id,
       system_icon_id,
       headou_id,
       leader_id,
       teamname,
       description,
       grouprule,
       code,
       dutygroup,
       maingroup,
       headgroup,
       role_linktoprocess,
       title,
       insert_date,
       `date`
FROM ${ods_schema_name}.tbl_team_delta_tmp;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_team_delta_tmp;