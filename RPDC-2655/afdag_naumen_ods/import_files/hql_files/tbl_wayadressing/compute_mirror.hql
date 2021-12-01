INSERT OVERWRITE TABLE ${ods_schema_name}.tbl_wayadressing
            SELECT '${dws_job}' as dws_job,insert_date,deleted_flag,nk,default_flag,id,removal_date,removed,code,color,folder,pos,parent,title_en,title_client,title FROM
            (SELECT *, row_number() over (partition by id order by insert_date desc) as row_num 
                FROM (
                    SELECT 
                    dws_job, deleted_flag, insert_date, nk, default_flag,
                    id, removal_date, removed, code, color, folder, pos, parent, title_en, title_client, title
                    FROM ${ods_schema_name}.tbl_wayadressing
                    UNION ALL
                    SELECT 
                    dws_job, CASE WHEN (dws_act = 'D') THEN 'Y' ELSE 'N' END as deleted_flag, '${insert_date}' as insert_date, nk, "N" as default_flag,
                    id, removal_date, removed, code, color, folder, pos, parent, title_en, title_client, title 
                    FROM ${ods_schema_name}.tbl_wayadressing_delta
                    WHERE date = '${insert_date_delta}'
                ) union_delta_mirror) mirror where row_num = 1