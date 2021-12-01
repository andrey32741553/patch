FROM (SELECT *, 
                      count(id) over (partition by id order by insert_date desc) as count_num,
                      row_number() over (partition by id order by insert_date desc) as row_num FROM
                (SELECT 
                    dws_job, insert_date, dws_act, nk, dws_uniact,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, city_phone, date_of_birth, email, first_name, home_phone, internal_phone, last_name, login, middle_name, mobile_phone, num_, password, post, private_code, author_id, system_icon_id, parent_id, location, timezone, idholder, headou, iternum, postelement, dn, is_employee_locked, password_change_date, password_must_be_changed, immheaduser, lastlogindate, room, intervalstart, intervalend, comment_author_alias, is_employee_for_integration
                FROM (SELECT coalesce(mirror.nk, reflect("java.util.UUID", "randomUUID")) AS nk,
                             '${dws_job}'                                AS dws_job,
                             '${insert_date}'                              AS insert_date,
                             'N'                                     AS dws_uniact,
                             CASE
                                 WHEN ((mirror.id is null) or (mirror.id = src.id and mirror.deleted_flag = 'Y' and '${insert_date}' > mirror.insert_date))
                                     THEN 'I'
                                 WHEN (src.id is null and mirror.deleted_flag = 'N')
                                     THEN 'D'
                                 WHEN (mirror.deleted_flag = 'N') and ((src.creation_date <=> mirror.creation_date) = FALSE or (src.last_modified_date <=> mirror.last_modified_date) = FALSE or (src.removal_date <=> mirror.removal_date) = FALSE or (src.removed <=> mirror.removed) = FALSE or (src.title <=> mirror.title) = FALSE or (src.case_id <=> mirror.case_id) = FALSE or (src.city_phone <=> mirror.city_phone) = FALSE or (src.date_of_birth <=> mirror.date_of_birth) = FALSE or (src.email <=> mirror.email) = FALSE or (src.first_name <=> mirror.first_name) = FALSE or (src.home_phone <=> mirror.home_phone) = FALSE or (src.internal_phone <=> mirror.internal_phone) = FALSE or (src.last_name <=> mirror.last_name) = FALSE or (src.login <=> mirror.login) = FALSE or (src.middle_name <=> mirror.middle_name) = FALSE or (src.mobile_phone <=> mirror.mobile_phone) = FALSE or (src.num_ <=> mirror.num_) = FALSE or (src.password <=> mirror.password) = FALSE or (src.post <=> mirror.post) = FALSE or (src.private_code <=> mirror.private_code) = FALSE or (src.author_id <=> mirror.author_id) = FALSE or (src.system_icon_id <=> mirror.system_icon_id) = FALSE or (src.parent_id <=> mirror.parent_id) = FALSE or (src.location <=> mirror.location) = FALSE or (src.timezone <=> mirror.timezone) = FALSE or (src.idholder <=> mirror.idholder) = FALSE or (src.headou <=> mirror.headou) = FALSE or (src.iternum <=> mirror.iternum) = FALSE or (src.postelement <=> mirror.postelement) = FALSE or (src.dn <=> mirror.dn) = FALSE or (src.is_employee_locked <=> mirror.is_employee_locked) = FALSE or (src.password_change_date <=> mirror.password_change_date) = FALSE or (src.password_must_be_changed <=> mirror.password_must_be_changed) = FALSE or (src.immheaduser <=> mirror.immheaduser) = FALSE or (src.lastlogindate <=> mirror.lastlogindate) = FALSE or (src.room <=> mirror.room) = FALSE or (src.intervalstart <=> mirror.intervalstart) = FALSE or (src.intervalend <=> mirror.intervalend) = FALSE or (src.comment_author_alias <=> mirror.comment_author_alias) = FALSE or (src.is_employee_for_integration <=> mirror.is_employee_for_integration) = FALSE) 
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
                             THEN src.city_phone
                         WHEN (mirror.id is not null)
                             THEN mirror.city_phone
                         ELSE null
                     END AS city_phone
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.date_of_birth
                         WHEN (mirror.id is not null)
                             THEN mirror.date_of_birth
                         ELSE null
                     END AS date_of_birth
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.email
                         WHEN (mirror.id is not null)
                             THEN mirror.email
                         ELSE null
                     END AS email
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.first_name
                         WHEN (mirror.id is not null)
                             THEN mirror.first_name
                         ELSE null
                     END AS first_name
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.home_phone
                         WHEN (mirror.id is not null)
                             THEN mirror.home_phone
                         ELSE null
                     END AS home_phone
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.internal_phone
                         WHEN (mirror.id is not null)
                             THEN mirror.internal_phone
                         ELSE null
                     END AS internal_phone
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.last_name
                         WHEN (mirror.id is not null)
                             THEN mirror.last_name
                         ELSE null
                     END AS last_name
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.login
                         WHEN (mirror.id is not null)
                             THEN mirror.login
                         ELSE null
                     END AS login
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.middle_name
                         WHEN (mirror.id is not null)
                             THEN mirror.middle_name
                         ELSE null
                     END AS middle_name
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.mobile_phone
                         WHEN (mirror.id is not null)
                             THEN mirror.mobile_phone
                         ELSE null
                     END AS mobile_phone
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
                             THEN src.password
                         WHEN (mirror.id is not null)
                             THEN mirror.password
                         ELSE null
                     END AS password
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.post
                         WHEN (mirror.id is not null)
                             THEN mirror.post
                         ELSE null
                     END AS post
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.private_code
                         WHEN (mirror.id is not null)
                             THEN mirror.private_code
                         ELSE null
                     END AS private_code
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
                             THEN src.parent_id
                         WHEN (mirror.id is not null)
                             THEN mirror.parent_id
                         ELSE null
                     END AS parent_id
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
                             THEN src.timezone
                         WHEN (mirror.id is not null)
                             THEN mirror.timezone
                         ELSE null
                     END AS timezone
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
                             THEN src.headou
                         WHEN (mirror.id is not null)
                             THEN mirror.headou
                         ELSE null
                     END AS headou
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.iternum
                         WHEN (mirror.id is not null)
                             THEN mirror.iternum
                         ELSE null
                     END AS iternum
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.postelement
                         WHEN (mirror.id is not null)
                             THEN mirror.postelement
                         ELSE null
                     END AS postelement
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.dn
                         WHEN (mirror.id is not null)
                             THEN mirror.dn
                         ELSE null
                     END AS dn
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.is_employee_locked
                         WHEN (mirror.id is not null)
                             THEN mirror.is_employee_locked
                         ELSE null
                     END AS is_employee_locked
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.password_change_date
                         WHEN (mirror.id is not null)
                             THEN mirror.password_change_date
                         ELSE null
                     END AS password_change_date
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.password_must_be_changed
                         WHEN (mirror.id is not null)
                             THEN mirror.password_must_be_changed
                         ELSE null
                     END AS password_must_be_changed
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.immheaduser
                         WHEN (mirror.id is not null)
                             THEN mirror.immheaduser
                         ELSE null
                     END AS immheaduser
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.lastlogindate
                         WHEN (mirror.id is not null)
                             THEN mirror.lastlogindate
                         ELSE null
                     END AS lastlogindate
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.room
                         WHEN (mirror.id is not null)
                             THEN mirror.room
                         ELSE null
                     END AS room
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.intervalstart
                         WHEN (mirror.id is not null)
                             THEN mirror.intervalstart
                         ELSE null
                     END AS intervalstart
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.intervalend
                         WHEN (mirror.id is not null)
                             THEN mirror.intervalend
                         ELSE null
                     END AS intervalend
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.comment_author_alias
                         WHEN (mirror.id is not null)
                             THEN mirror.comment_author_alias
                         ELSE null
                     END AS comment_author_alias
        , 
                     CASE
                         WHEN (src.id is not null)
                             THEN src.is_employee_for_integration
                         WHEN (mirror.id is not null)
                             THEN mirror.is_employee_for_integration
                         ELSE null
                     END AS is_employee_for_integration
        
                      FROM ${ods_schema_name}.tbl_employee_src AS src
                      FULL OUTER JOIN ${ods_schema_name}.tbl_employee 
                      AS mirror ON (src.id = mirror.id)) delta WHERE delta.dws_act <> "") delta_num ) rsl

                INSERT INTO TABLE ${ods_schema_name}.tbl_employee_delta partition (`date`)
                    SELECT dws_job, dws_act, nk, dws_uniact,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, city_phone, date_of_birth, email, first_name, home_phone, internal_phone, last_name, login, middle_name, mobile_phone, num_, password, post, private_code, author_id, system_icon_id, parent_id, location, timezone, idholder, headou, iternum, postelement, dn, is_employee_locked, password_change_date, password_must_be_changed, immheaduser, lastlogindate, room, intervalstart, intervalend, comment_author_alias, is_employee_for_integration, 
                    insert_date, '${date}' as `date`
                WHERE row_num = 1

                INSERT INTO TABLE ${ods_schema_name}.tbl_employee_double
                    SELECT dws_job,
                    insert_date as as_of_date,
                    CASE 
                        WHEN (row_num = 1) THEN 'Y'
                    ELSE 'N' END AS effective_flag,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, city_phone, date_of_birth, email, first_name, home_phone, internal_phone, last_name, login, middle_name, mobile_phone, num_, password, post, private_code, author_id, system_icon_id, parent_id, location, timezone, idholder, headou, iternum, postelement, dn, is_employee_locked, password_change_date, password_must_be_changed, immheaduser, lastlogindate, room, intervalstart, intervalend, comment_author_alias, is_employee_for_integration
                    WHERE count_num > 1;