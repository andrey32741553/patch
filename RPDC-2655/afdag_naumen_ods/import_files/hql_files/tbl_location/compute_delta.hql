FROM
(SELECT *,
        count(id) over (partition by id order by insert_date desc)    as count_num,
        row_number() over (partition by id order by insert_date desc) as row_num
 FROM (SELECT dws_job,
              insert_date,
              dws_act,
              nk,
              CASE
                  WHEN (dws_act = 'I')
                      THEN 'I'
                  WHEN (dws_act = 'D'  and change_logic_key = TRUE)
                      THEN 'U'
                  WHEN (dws_act = 'D'  and change_logic_key = FALSE)
                      THEN 'N'
                  WHEN (dws_act = 'U'  and change_logic_key = TRUE)
                      THEN 'U'
                  WHEN (dws_act = 'U'  and change_logic_key = FALSE)
                      THEN 'N'
              END as dws_uniact,
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
              connectoo
       FROM (SELECT coalesce(mirror.nk, reflect("java.util.UUID", "randomUUID")) AS nk,
                    '${dws_job}'                                                 AS dws_job,
                    '${insert_date}'                                             AS insert_date,


                    ((src.idholder <=> mirror.idholder)=FALSE) as change_logic_key,

                    CASE
                        WHEN ((mirror.id is null) or (mirror.id = src.id
                                                          and mirror.deleted_flag = 'Y'
                                                          and src.removed = FALSE
                                                          and '${insert_date}' > mirror.insert_date))
                            THEN 'I'
                        WHEN ((src.id is null and mirror.deleted_flag = 'N') or (src.removed = TRUE and mirror.deleted_flag = 'N'))
                            THEN 'D'

                        WHEN (mirror.deleted_flag = 'N' and src.removed = FALSE) and ((src.creation_date <=> mirror.creation_date) = FALSE or
                                                              (src.last_modified_date <=> mirror.last_modified_date) =
                                                              FALSE or
                                                              (src.removal_date <=> mirror.removal_date) = FALSE or
                                                              (src.removed <=> mirror.removed) = FALSE or
                                                              (src.title <=> mirror.title) = FALSE or
                                                              (src.case_id <=> mirror.case_id) = FALSE or
                                                              (src.num_ <=> mirror.num_) = FALSE or
                                                              (src.responsiblestarttime <=> mirror.responsiblestarttime) =
                                                              FALSE or (src.state <=> mirror.state) = FALSE or
                                                              (src.statestarttime <=> mirror.statestarttime) = FALSE or
                                                              (src.parent <=> mirror.parent) = FALSE or
                                                              (src.address <=> mirror.address) = FALSE or
                                                              (src.postoffice_typecode <=> mirror.postoffice_typecode) =
                                                              FALSE or
                                                              (src.postoffice_postalcodetype <=> mirror.postoffice_postalcodetype) =
                                                              FALSE or
                                                              (src.postoffice_temporaryworkd <=> mirror.postoffice_temporaryworkd) =
                                                              FALSE or
                                                              (src.postoffice_priorityoo <=> mirror.postoffice_priorityoo) =
                                                              FALSE or
                                                              (src.postoffice_servzone <=> mirror.postoffice_servzone) =
                                                              FALSE or
                                                              (src.postoffice_lon <=> mirror.postoffice_lon) = FALSE or
                                                              (src.postoffice_temporaryworkr <=> mirror.postoffice_temporaryworkr) =
                                                              FALSE or
                                                              (src.postoffice_typename <=> mirror.postoffice_typename) =
                                                              FALSE or
                                                              (src.postoffice_lat <=> mirror.postoffice_lat) = FALSE or
                                                              (src.postoffice_postid <=> mirror.postoffice_postid) =
                                                              FALSE or
                                                              (src.postoffice_postalcodeptn <=> mirror.postoffice_postalcodeptn) =
                                                              FALSE or
                                                              (src.postoffice_postcode <=> mirror.postoffice_postcode) =
                                                              FALSE or
                                                              (src.postoffice_temporaryclosr <=> mirror.postoffice_temporaryclosr) =
                                                              FALSE or
                                                              (src.postoffice_shortname <=> mirror.postoffice_shortname) =
                                                              FALSE or
                                                              (src.postoffice_servicetime <=> mirror.postoffice_servicetime) =
                                                              FALSE or
                                                              (src.postoffice_postalcodetc <=> mirror.postoffice_postalcodetc) =
                                                              FALSE or
                                                              (src.postoffice_description <=> mirror.postoffice_description) =
                                                              FALSE or
                                                              (src.postoffice_privatecategor <=> mirror.postoffice_privatecategor) =
                                                              FALSE or
                                                              (src.postoffice_postalclass <=> mirror.postoffice_postalclass) =
                                                              FALSE or
                                                              (src.postoffice_nearestofficep <=> mirror.postoffice_nearestofficep) =
                                                              FALSE or
                                                              (src.postoffice_posttypeid <=> mirror.postoffice_posttypeid) =
                                                              FALSE or
                                                              (src.postoffice_temporaryclosd <=> mirror.postoffice_temporaryclosd) =
                                                              FALSE or
                                                              (src.postoffice_parentops <=> mirror.postoffice_parentops) =
                                                              FALSE or
                                                              (src.postoffice_istemporaryclo <=> mirror.postoffice_istemporaryclo) =
                                                              FALSE or
                                                              (src.postoffice_isclosed <=> mirror.postoffice_isclosed) =
                                                              FALSE or
                                                              (src.postoffice_exactprecision <=> mirror.postoffice_exactprecision) =
                                                              FALSE or (src.author_id <=> mirror.author_id) = FALSE or
                                                              (src.system_icon_id <=> mirror.system_icon_id) = FALSE or
                                                              (src.responsibleemployee_id <=> mirror.responsibleemployee_id) =
                                                              FALSE or
                                                              (src.responsibleteam_id <=> mirror.responsibleteam_id) =
                                                              FALSE or (src.timezone <=> mirror.timezone) = FALSE or
                                                              (src.idholder <=> mirror.idholder) = FALSE or
                                                              (src.postoffice_location <=> mirror.postoffice_location) =
                                                              FALSE or (src.connectoo <=> mirror.connectoo) = FALSE)
                            THEN 'U'
                        ELSE ''
                        END                                                      AS dws_act,

                    CASE
                        WHEN (src.id is not null)
                            THEN src.id
                        WHEN (mirror.id is not null)
                            THEN mirror.id
                        ELSE null
                        END                                                      AS id
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.creation_date
                        WHEN (mirror.id is not null)
                            THEN mirror.creation_date
                        ELSE null
                        END                                                      AS creation_date
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.last_modified_date
                        WHEN (mirror.id is not null)
                            THEN mirror.last_modified_date
                        ELSE null
                        END                                                      AS last_modified_date
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.removal_date
                        WHEN (mirror.id is not null)
                            THEN mirror.removal_date
                        ELSE null
                        END                                                      AS removal_date
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.removed
                        WHEN (mirror.id is not null)
                            THEN mirror.removed
                        ELSE null
                        END                                                      AS removed
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.title
                        WHEN (mirror.id is not null)
                            THEN mirror.title
                        ELSE null
                        END                                                      AS title
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.case_id
                        WHEN (mirror.id is not null)
                            THEN mirror.case_id
                        ELSE null
                        END                                                      AS case_id
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.num_
                        WHEN (mirror.id is not null)
                            THEN mirror.num_
                        ELSE null
                        END                                                      AS num_
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.responsiblestarttime
                        WHEN (mirror.id is not null)
                            THEN mirror.responsiblestarttime
                        ELSE null
                        END                                                      AS responsiblestarttime
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.state
                        WHEN (mirror.id is not null)
                            THEN mirror.state
                        ELSE null
                        END                                                      AS state
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.statestarttime
                        WHEN (mirror.id is not null)
                            THEN mirror.statestarttime
                        ELSE null
                        END                                                      AS statestarttime
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.parent
                        WHEN (mirror.id is not null)
                            THEN mirror.parent
                        ELSE null
                        END                                                      AS parent
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.address
                        WHEN (mirror.id is not null)
                            THEN mirror.address
                        ELSE null
                        END                                                      AS address
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_typecode
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_typecode
                        ELSE null
                        END                                                      AS postoffice_typecode
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_postalcodetype
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_postalcodetype
                        ELSE null
                        END                                                      AS postoffice_postalcodetype
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_temporaryworkd
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_temporaryworkd
                        ELSE null
                        END                                                      AS postoffice_temporaryworkd
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_priorityoo
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_priorityoo
                        ELSE null
                        END                                                      AS postoffice_priorityoo
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_servzone
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_servzone
                        ELSE null
                        END                                                      AS postoffice_servzone
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_lon
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_lon
                        ELSE null
                        END                                                      AS postoffice_lon
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_temporaryworkr
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_temporaryworkr
                        ELSE null
                        END                                                      AS postoffice_temporaryworkr
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_typename
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_typename
                        ELSE null
                        END                                                      AS postoffice_typename
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_lat
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_lat
                        ELSE null
                        END                                                      AS postoffice_lat
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_postid
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_postid
                        ELSE null
                        END                                                      AS postoffice_postid
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_postalcodeptn
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_postalcodeptn
                        ELSE null
                        END                                                      AS postoffice_postalcodeptn
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_postcode
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_postcode
                        ELSE null
                        END                                                      AS postoffice_postcode
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_temporaryclosr
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_temporaryclosr
                        ELSE null
                        END                                                      AS postoffice_temporaryclosr
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_shortname
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_shortname
                        ELSE null
                        END                                                      AS postoffice_shortname
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_servicetime
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_servicetime
                        ELSE null
                        END                                                      AS postoffice_servicetime
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_postalcodetc
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_postalcodetc
                        ELSE null
                        END                                                      AS postoffice_postalcodetc
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_description
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_description
                        ELSE null
                        END                                                      AS postoffice_description
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_privatecategor
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_privatecategor
                        ELSE null
                        END                                                      AS postoffice_privatecategor
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_postalclass
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_postalclass
                        ELSE null
                        END                                                      AS postoffice_postalclass
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_nearestofficep
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_nearestofficep
                        ELSE null
                        END                                                      AS postoffice_nearestofficep
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_posttypeid
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_posttypeid
                        ELSE null
                        END                                                      AS postoffice_posttypeid
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_temporaryclosd
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_temporaryclosd
                        ELSE null
                        END                                                      AS postoffice_temporaryclosd
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_parentops
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_parentops
                        ELSE null
                        END                                                      AS postoffice_parentops
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_istemporaryclo
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_istemporaryclo
                        ELSE null
                        END                                                      AS postoffice_istemporaryclo
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_isclosed
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_isclosed
                        ELSE null
                        END                                                      AS postoffice_isclosed
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_exactprecision
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_exactprecision
                        ELSE null
                        END                                                      AS postoffice_exactprecision
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.author_id
                        WHEN (mirror.id is not null)
                            THEN mirror.author_id
                        ELSE null
                        END                                                      AS author_id
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.system_icon_id
                        WHEN (mirror.id is not null)
                            THEN mirror.system_icon_id
                        ELSE null
                        END                                                      AS system_icon_id
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.responsibleemployee_id
                        WHEN (mirror.id is not null)
                            THEN mirror.responsibleemployee_id
                        ELSE null
                        END                                                      AS responsibleemployee_id
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.responsibleteam_id
                        WHEN (mirror.id is not null)
                            THEN mirror.responsibleteam_id
                        ELSE null
                        END                                                      AS responsibleteam_id
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.timezone
                        WHEN (mirror.id is not null)
                            THEN mirror.timezone
                        ELSE null
                        END                                                      AS timezone
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.idholder
                        WHEN (mirror.id is not null)
                            THEN mirror.idholder
                        ELSE null
                        END                                                      AS idholder
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.postoffice_location
                        WHEN (mirror.id is not null)
                            THEN mirror.postoffice_location
                        ELSE null
                        END                                                      AS postoffice_location
                     ,
                    CASE
                        WHEN (src.id is not null)
                            THEN src.connectoo
                        WHEN (mirror.id is not null)
                            THEN mirror.connectoo
                        ELSE null
                        END                                                      AS connectoo

             FROM ${ods_schema_name}.tbl_location_src AS src
                      FULL OUTER JOIN ${ods_schema_name}.tbl_location
                 AS mirror ON (src.id = mirror.id)) delta
       WHERE delta.dws_act <> "") delta_num

    ) rsl




INSERT INTO TABLE ${ods_schema_name}.tbl_location_delta partition (`date`)
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
       '${date}' as `date`
WHERE row_num = 1

INSERT INTO TABLE ${ods_schema_name}.tbl_location_double
SELECT dws_job,
       insert_date      as as_of_date,
       CASE
           WHEN (row_num = 1) THEN 'Y'
           ELSE 'N' END AS effective_flag,
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
       connectoo
WHERE count_num > 1;