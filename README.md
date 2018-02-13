# checksql
Utility to validate SQL queries against DB schema. 
It extracts SQL from known list of DB tables and validates it by creating View for "select" statements and procedure for PL/SQL and then evaluating creation errors.
SQL statements may be tested in DB schema which is differs from the one where it extracted from, this allows to test some user configurable SQL before upgrade to ensure it is compatible with new DB structure.

To start create checksql.json file in working directory and set DB connection string for source DB schema where SQL statements will be extracted in <remote_owner> parameter,
and DB connection string for DB where SQL statements will be tested in <remote_user> parameter. Values may be the same. See examples bellow:

java -jar check-sql.jar vqs_p01_epm/vepm@192.168.56.101:1521:xe vqs_p01_epm/vepm@192.168.56.101:1521:xe
java -jar check-sql.jar vqs_p01_epm/vepm@192.168.56.101:1521:xe

After start app will print progress, summary and additional information to the standard output. Summary will contain info on each table tested and may look like this:
```
========TABLE STATS========= 
table=config_field, err-count=14 
table=excel_orch_mapping, err-count=0 
table=grid_page_field, err-count=0 
table=imp_data_map, err-count=2 
table=imp_data_type, err-count=17 
table=imp_data_type_param, err-count=19 
table=imp_entity, err-count=3 
table=imp_entity_req_field, err-count=0 
table=imp_spec, err-count=3 
table=notif, err-count=0 
table=report_lookup, err-count=11 
table=report_sql, err-count=0 
table=rule, err-count=3 
table=rule_class_param, err-count=0 
table=rule_class_param_value, err-count=0 
table=rule_type, err-count=0 
table=tm_setup, err-count=0 
table=wf_step, err-count=0 
table=wf_template_step, err-count=0 
table=xitor_req_field, err-count=0 
SQL Checker is completed 
```

Also, output duplicated in logs/*_info.log file. 

After checksql completion logs/*_data.log file should be evaluated for error details and be used as starting point to fix broken SQLs
