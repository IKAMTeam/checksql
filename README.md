# checksql
Utility to validate SQL queries against DB schema. 
It extracts SQL from known list of DB tables and validates it by creating View for "select" statements and procedure for PL/SQL and then evaluating creation errors.
SQL statements may be tested in DB schema which is differs from the one where it extracted from, this allows to test some user configurable SQL before upgrade to ensure it is compatible with new DB structure.

To start create checksql.json file in working directory.

Structure of checksql.json file:
```json
[
  {"table.column": "sql select statement"},
  "table.column"
]
```

Set DB connection string for source DB schema where SQL statements will be extracted in <remote_owner> parameter, and DB connection string for DB where SQL statements will be tested in <local_owner> parameter. Values may be the same. See examples bellow:

```
java -jar checksql.jar <remote_owner>/<password>@<connect_identifier> [<local_owner>/<password>@<connect_identifier>] [path_to_config_file]

java -jar checksql.jar vqs_p01_epm/****@192.168.56.101:1521:xe vqs_p01_epm/****@192.168.56.101:1521:xe

java -jar checksql.jar vqs_p01_epm/****@192.168.56.101:1521:xe

java -jar checksql.jar vqs_p01_epm/****@192.168.56.101:1521:xe /home/test/checksql.json
```

After start app will print progress, summary and additional information to the standard output. Summary will contain info on each table tested and may look like this:
```
========checksql Summary========= 
Passed (table name, rows checked): 
config_field, 24 
grid_page_field, 2 
imp_data_map, 1 
imp_data_type, 25 
imp_data_type_param, 62 
imp_entity, 47 
imp_entity_req_field, 0 
imp_spec, 32 
notif, 5 
report_lookup, 41 
report_sql, 87 
rule, 19 
rule_class_param, 30 
rule_class_param_value, 0 
rule_type, 22 
tm_setup, 3 
wf_step, 0 
wf_template_step, 0 

Failed (table name, errors count): 
config_field, 14 
imp_data_map, 1 
imp_data_type, 15 
imp_data_type_param, 14 
imp_entity, 6 
imp_spec, 3 
report_lookup, 11 
rule, 3 
```

Also, output duplicated in logs/*_info.log file. 

After checksql completion logs/*_data.log file should be evaluated for error details and be used as starting point to fix broken SQLs
