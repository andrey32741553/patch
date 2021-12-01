INSERT OVERWRITE TABLE ${ods_schema_name}.tbl_service
            SELECT '${dws_job}' as dws_job,insert_date,deleted_flag,nk,default_flag,id,creation_date,last_modified_date,removal_date,removed,title,case_id,num_,responsiblestarttime,state,statestarttime,inventorynum,description,groupexprfc,groupdevman,groupregrfcman,idholder,parent,groupitservman,techservice_provider,author_id,system_icon_id,responsibleemployee_id,responsibleteam_id,emailsc,compcheck,groupbisclient,groupresprfc,negotstaddtkt,defstart,resippolice,rtfallow,grpallow,locallow,grpdesc,flallow,ciallow,prefix,canchecksn,monitoringlink,archiveaction,stabilmangroup,mangroupinv,assetpolicy,usericon,routcinotfound,groupregisprb,prbregispolicy FROM
            (SELECT *, row_number() over (partition by id order by insert_date desc) as row_num 
                FROM (
                    SELECT 
                    dws_job, deleted_flag, insert_date, nk, default_flag,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, num_, responsiblestarttime, state, statestarttime, inventorynum, description, groupexprfc, groupdevman, groupregrfcman, idholder, parent, groupitservman, techservice_provider, author_id, system_icon_id, responsibleemployee_id, responsibleteam_id, emailsc, compcheck, groupbisclient, groupresprfc, negotstaddtkt, defstart, resippolice, rtfallow, grpallow, locallow, grpdesc, flallow, ciallow, prefix, canchecksn, monitoringlink, archiveaction, stabilmangroup, mangroupinv, assetpolicy, usericon, routcinotfound, groupregisprb, prbregispolicy
                    FROM ${ods_schema_name}.tbl_service
                    UNION ALL
                    SELECT 
                    dws_job, CASE WHEN (dws_act = 'D') THEN 'Y' ELSE 'N' END as deleted_flag, '${insert_date}' as insert_date, nk, "N" as default_flag,
                    id, creation_date, last_modified_date, removal_date, removed, title, case_id, num_, responsiblestarttime, state, statestarttime, inventorynum, description, groupexprfc, groupdevman, groupregrfcman, idholder, parent, groupitservman, techservice_provider, author_id, system_icon_id, responsibleemployee_id, responsibleteam_id, emailsc, compcheck, groupbisclient, groupresprfc, negotstaddtkt, defstart, resippolice, rtfallow, grpallow, locallow, grpdesc, flallow, ciallow, prefix, canchecksn, monitoringlink, archiveaction, stabilmangroup, mangroupinv, assetpolicy, usericon, routcinotfound, groupregisprb, prbregispolicy 
                    FROM ${ods_schema_name}.tbl_service_delta
                    WHERE date = '${insert_date_delta}'
                ) union_delta_mirror) mirror where row_num = 1