sqoop import -Dmapreduce.job.user.classpath.first=true -Dmapreduce.map.maxattempts=20
--connect {$sqoop_naumen_jdbc}
--username {$sqoop_naumen_user}
--password {$sqoop_naumen_password}
--num-mappers 1
--query "SELECT
servicecall_id,
analyticalfeat_id,
1 as snapshot,
FALSE as deleted_flag,
'naumen' as system_id,
'{$creation_date}' as creation_date
FROM tbl_servicec_analytic WHERE \$CONDITIONS"
--hcatalog-database {$hadoop_database} 
--hcatalog-table {$hadoop_table_name}
--hcatalog-storage-stanza 'stored as orcfile'