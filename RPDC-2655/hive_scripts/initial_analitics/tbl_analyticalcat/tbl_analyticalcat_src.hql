DROP TABLE IF EXISTS ${ods_schema_name}.tbl_analyticalcat_src;

CREATE TABLE IF NOT EXISTS ${ods_schema_name}.tbl_analyticalcat_src
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
    parent                 bigint,
    author_id              bigint,
    system_icon_id         bigint,
    responsibleemployee_id bigint,
    responsibleteam_id     bigint,
    headcat                bigint,
    visor                  bigint,
    catalog_needcheck      boolean,
    catalog_needcopy       boolean,
    snapshot               int,
    deleted_flag           string,
    system_id              string
)
    STORED AS ORC;