INSERT OVERWRITE TABLE ${ods_schema_name}.tbl_team
            SELECT '${dws_job}' as dws_job,insert_date,deleted_flag,nk,default_flag,id,creation_date,last_modified_date,removal_date,removed,case_id,num_,author_id,system_icon_id,headou_id,leader_id,teamname,description,grouprule,code,dutygroup,maingroup,headgroup,role_linktoprocess,title FROM
            (SELECT *, row_number() over (partition by id order by insert_date desc) as row_num 
                FROM (
                    SELECT 
                    dws_job, deleted_flag, insert_date, nk, default_flag,
                    id, creation_date, last_modified_date, removal_date, removed, case_id, num_, author_id, system_icon_id, headou_id, leader_id, teamname, description, grouprule, code, dutygroup, maingroup, headgroup, role_linktoprocess, title
                    FROM ${ods_schema_name}.tbl_team
                    UNION ALL
                    SELECT 
                    dws_job, CASE WHEN (dws_act = 'D') THEN 'Y' ELSE 'N' END as deleted_flag, '${insert_date}' as insert_date, nk, "N" as default_flag,
                    id, creation_date, last_modified_date, removal_date, removed, case_id, num_, author_id, system_icon_id, headou_id, leader_id, teamname, description, grouprule, code, dutygroup, maingroup, headgroup, role_linktoprocess, title 
                    FROM ${ods_schema_name}.tbl_team_delta
                    WHERE date = '${insert_date_delta}'
                ) union_delta_mirror) mirror where row_num = 1