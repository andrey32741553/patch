DROP TABLE IF EXISTS ${ods_schema_name}.tbl_analyticalcat_delta;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_analyticalcat_delta
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
    parent                 bigint,
    author_id              bigint,
    system_icon_id         bigint,
    responsibleemployee_id bigint,
    responsibleteam_id     bigint,
    headcat                bigint,
    visor                  bigint,
    catalog_needcheck      boolean,
    catalog_needcopy       boolean,
    insert_date            string
) PARTITIONED BY (`date` string)
    STORED AS ORC;