set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_impact_delta_tmp
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
    title        string,
    insert_date  string
)
    PARTITIONED BY (`date` string)
    STORED AS ORC;

INSERT INTO TABLE ${ods_schema_name}.tbl_impact_delta_tmp partition (`date`)
(dws_job,
 dws_act,
 nk,
 dws_uniact,
 id,
 removal_date,
 removed,
 code,
 color,
 folder,
 pos,
 parent,
 title_en,
 title_client,
 title,
 insert_date,
 `date`)
SELECT dws_job,
       dws_act,
       nk,
       dws_uniact,
       id,
       removal_date,
       removed,
       code,
       color,
       folder,
       pos,
       parent,
       title_en,
       title_client,
       title,
       insert_date,
       SUBSTRING(insert_date, 1, 10) as `date`
FROM ${ods_schema_name}.tbl_impact_delta;

DROP TABLE IF EXISTS ${ods_schema_name}.tbl_impact_delta;