sqoop import -Dmapreduce.job.user.classpath.first=true -Dmapreduce.map.maxattempts=20
--connect {$sqoop_naumen_jdbc}
--username {$sqoop_naumen_user}
--password {$sqoop_naumen_password}
--num-mappers 1
--query "SELECT
id,
TO_CHAR(creation_date, 'yyyy-MM-dd HH24:MI:SS.US') as creation_date,
TO_CHAR(last_modified_date, 'yyyy-MM-dd HH24:MI:SS.US') as last_modified_date,
TO_CHAR(removal_date, 'yyyy-MM-dd HH24:MI:SS.US') as removal_date,
removed,
title,
case_id,
num_,
TO_CHAR(responsiblestarttime, 'yyyy-MM-dd HH24:MI:SS.US') as responsiblestarttime,
state,
TO_CHAR(statestarttime, 'yyyy-MM-dd HH24:MI:SS.US') as statestarttime,
parent,
author_id,
system_icon_id,
responsibleemployee_id,
responsibleteam_id,
headcat,
visor,
catalog\$needcheck as catalog_needcheck,
catalog\$needcopy as catalog_needcopy,
1 as snapshot,
FALSE as deleted_flag,
'naumen' as system_id
FROM tbl_analyticalcat WHERE \$CONDITIONS"
--hcatalog-database {$hadoop_database} 
--hcatalog-table {$hadoop_table_name}
--hcatalog-storage-stanza 'stored as orcfile'