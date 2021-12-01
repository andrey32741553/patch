INSERT OVERWRITE TABLE ${ods_schema_name}.tbl_employee
            SELECT '${dws_job}' as dws_job,insert_date,deleted_flag,nk,default_flag,id,creation_date,last_modified_date,removal_date,removed,title,case_id,city_phone,date_of_birth,email,first_name,home_phone,internal_phone,last_name,login,middle_name,mobile_phone,num_,password,post,private_code,author_id,system_icon_id,parent_id,location,timezone,idholder,headou,iternum,postelement,dn,is_employee_locked,password_change_date,password_must_be_changed,immheaduser,lastlogindate,room,intervalstart,intervalend,comment_author_alias,is_employee_for_integration FROM
            (SELECT *, row_number() over (partition by id order by insert_date desc) as row_num 
                FROM (
                    SELECT 
                    dws_job, deleted_flag, insert_date, nk, default_flag,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, city_phone, date_of_birth, email, first_name, home_phone, internal_phone, last_name, login, middle_name, mobile_phone, num_, password, post, private_code, author_id, system_icon_id, parent_id, location, timezone, idholder, headou, iternum, postelement, dn, is_employee_locked, password_change_date, password_must_be_changed, immheaduser, lastlogindate, room, intervalstart, intervalend, comment_author_alias, is_employee_for_integration
                    FROM ${ods_schema_name}.tbl_employee
                    UNION ALL
                    SELECT 
                    dws_job, CASE WHEN (dws_act = 'D') THEN 'Y' ELSE 'N' END as deleted_flag, '${insert_date}' as insert_date, nk, "N" as default_flag,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, city_phone, date_of_birth, email, first_name, home_phone, internal_phone, last_name, login, middle_name, mobile_phone, num_, password, post, private_code, author_id, system_icon_id, parent_id, location, timezone, idholder, headou, iternum, postelement, dn, is_employee_locked, password_change_date, password_must_be_changed, immheaduser, lastlogindate, room, intervalstart, intervalend, comment_author_alias, is_employee_for_integration 
                    FROM ${ods_schema_name}.tbl_employee_delta
                    WHERE date = '${insert_date_delta}'
                ) union_delta_mirror) mirror where row_num = 1