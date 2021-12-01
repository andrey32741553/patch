INSERT OVERWRITE TABLE ${ods_schema_name}.tbl_ou
            SELECT '${dws_job}' as dws_job,insert_date,deleted_flag,nk,default_flag,id,creation_date,last_modified_date,removal_date,removed,title,case_id,num_,author_id,system_icon_id,head_id,parent_id,accesskey,namefromad,idholder,shorttitle,accesskeyintrl,ouext_teampartners,location,updated,headou,ouext_syncuser FROM
            (SELECT *, row_number() over (partition by id order by insert_date desc) as row_num 
                FROM (
                    SELECT 
                    dws_job, deleted_flag, insert_date, nk, default_flag,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, num_, author_id, system_icon_id, head_id, parent_id, accesskey, namefromad, idholder, shorttitle, accesskeyintrl, ouext_teampartners, location, updated, headou, ouext_syncuser
                    FROM ${ods_schema_name}.tbl_ou
                    UNION ALL
                    SELECT 
                    dws_job, CASE WHEN (dws_act = 'D') THEN 'Y' ELSE 'N' END as deleted_flag,'${insert_date}' as  insert_date, nk, "N" as default_flag,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, num_, author_id, system_icon_id, head_id, parent_id, accesskey, namefromad, idholder, shorttitle, accesskeyintrl, ouext_teampartners, location, updated, headou, ouext_syncuser 
                    FROM ${ods_schema_name}.tbl_ou_delta
                    WHERE date = '${insert_date_delta}'
                ) union_delta_mirror) mirror where row_num = 1