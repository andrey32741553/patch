INSERT OVERWRITE TABLE ${ods_schema_name}.tbl_objectbase
            SELECT '${dws_job}' as dws_job,insert_date,deleted_flag,nk,default_flag,id,creation_date,last_modified_date,removal_date,removed,archqueue,author_id,case_id,cimodel,configitem_activeaudit,configitem_existstatelog,countcimass,datechcl,datechrep,datefinishexp,dateofservprov,datestartexp,dnsname,headplace,headslmservice,idholder,invunit_monitoringdate,invunitqueue,ip,lastauddate,lastcioffdate,lastclosedate,location,mac,num_,parent,prefix,responsibleemployee_id,responsiblestarttime,responsibleteam_id,sn,sourcecall,state,statestarttime,supservice,supsrvtech,title,vendor FROM
            (SELECT *, row_number() over (partition by id order by insert_date desc) as row_num 
                FROM (
                    SELECT 
                    dws_job, deleted_flag, insert_date, nk, default_flag,
                    id, creation_date, last_modified_date, removal_date, removed, archqueue, author_id, case_id, cimodel, configitem_activeaudit, configitem_existstatelog, countcimass, datechcl, datechrep, datefinishexp, dateofservprov, datestartexp, dnsname, headplace, headslmservice, idholder, invunit_monitoringdate, invunitqueue, ip, lastauddate, lastcioffdate, lastclosedate, location, mac, num_, parent, prefix, responsibleemployee_id, responsiblestarttime, responsibleteam_id, sn, sourcecall, state, statestarttime, supservice, supsrvtech, title, vendor
                    FROM ${ods_schema_name}.tbl_objectbase
                    UNION ALL
                    SELECT 
                    dws_job, CASE WHEN (dws_act = 'D') THEN 'Y' ELSE 'N' END as deleted_flag, '${insert_date}' as insert_date, nk, "N" as default_flag,
                    id, creation_date, last_modified_date, removal_date, removed, archqueue, author_id, case_id, cimodel, configitem_activeaudit, configitem_existstatelog, countcimass, datechcl, datechrep, datefinishexp, dateofservprov, datestartexp, dnsname, headplace, headslmservice, idholder, invunit_monitoringdate, invunitqueue, ip, lastauddate, lastcioffdate, lastclosedate, location, mac, num_, parent, prefix, responsibleemployee_id, responsiblestarttime, responsibleteam_id, sn, sourcecall, state, statestarttime, supservice, supsrvtech, title, vendor 
                    FROM ${ods_schema_name}.tbl_objectbase_delta
                    WHERE date = '${insert_date_delta}'
                ) union_delta_mirror) mirror where row_num = 1