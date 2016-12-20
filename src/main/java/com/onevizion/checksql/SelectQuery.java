package com.onevizion.checksql;

public enum SelectQuery {
    FIELD_DEF_SQL(true, "config_field", "config_field", "default_value_sql", "config_field_id", null),
    FIELD_SQL_QUERY(true, "config_field", "config_field", "sql_query", "config_field_id",
            "config_field_name <> 'XITOR_CLASS_ID'"),
    EXCEL_ORCH_DEF_SQL(true, "excel_orch_mapping", "excel_orch_mapping", "default_value_sql", "excel_orch_mapping_id",
            null),
    EXCEL_ORCH_SQL_QUERY(true, "excel_orch_mapping", "excel_orch_mapping", "sql_query", "excel_orch_mapping_id", null),
    PAGE_FIELD78(false, "grid_page_field", "grid_page_field", "cell_renderer_param1", "grid_page_field_id",
            "cell_renderer_id = 78"),
    IMP_ENTITY_REQ_FIELD(true, "imp_entity_req_field", "imp_entity_req_field", "sql_text", "imp_entity_req_field_id",
            "length(sql_text) > 0"),
    NOTIF_TRACKOR(true, "notif", "notif", "trackor_sql", "notif_id", null),
    NOTIF_USER(true, "notif", "notif", "user_sql", "notif_id", null),
    RPT_LOOKUP(true, "report_lookup", "report_lookup", "lookup_sql", "report_lookup_id", null),
    RPT_SQL(true, "report_sql", "report_sql", "sql_text", "report_sql_id", null),
    RULE_CLASS_PARAM(false, "rule_class_param", "rule_class_param", "sql_text", "rule_class_param_id", null),
    RULE_TYPE(false, "rule_type", "rule_type", "template_sql", "rule_type_id", null),
    TM_SETUP(true, "tm_setup", "tm_setup", "search_sql", "tm_setup_id", "length(search_sql) > 0"),
    XITOR_REQ_FIELD(true, "xitor_req_field", "xitor_req_field", "default_value_sql", "xitor_req_field_id", null),
    IMP_DATA_TYPE_PARAM(false, "imp_data_type_param", "imp_data_type_param", "sql_text", "imp_data_type_param_id",
            null),
    IMP_DATA_MAP(true, "imp_data_map", "imp_data_map", "sql_text", "imp_data_map_id", "length(sql_text) > 0"),
    IMP_ENTITY(true, "imp_entity", "imp_entity", "sql_text", "imp_entity_id", "dbms_lob.getlength(sql_text) > 0"),
    RULE_CLASS_PARAM_VAL(true, "rule_class_param_value",
            "rule_class_param_value v join rule r on (r.rule_id = v.rule_id)", "v.value_clob",
            "v.rule_class_param_value_id", "r.is_enabled = 1");

    public static final String TOTAL_ROWS_COL_NAME = "totalrows";

    private final boolean checkQuery;
    private final String fromClause;
    private final String sqlColName;
    private final String primKeyColName;
    private final String whereClause;
    private final String tableName;

    private SelectQuery(boolean checkQuery, String tableName, String fromClause, String sqlColName, String primKeyColName,
            String whereClause) {
        this.checkQuery = checkQuery;
        this.fromClause = fromClause;
        this.sqlColName = sqlColName;
        this.primKeyColName = primKeyColName;
        this.whereClause = whereClause;
        this.tableName = tableName;
    }

    public String getFromClause() {
        return fromClause;
    }

    public String getColumnName() {
        return sqlColName;
    }

    public String getPrimKeyColName() {
        return primKeyColName;
    }

    public boolean isCheckQuery() {
        return checkQuery;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSql() {
        StringBuilder sql = new StringBuilder("select ");
        sql.append(getPrimKeyColName());
        sql.append(", ");
        sql.append(getColumnName());
        sql.append(", count(*) over () as ");
        sql.append(TOTAL_ROWS_COL_NAME);
        sql.append(" from ");
        sql.append(getFromClause());
        sql.append(" where ");
        sql.append(getColumnName());
        sql.append(" is not null");
        if (getWhereClause() != null) {
            sql.append(" and ");
            sql.append(getWhereClause());
        }
        return sql.toString();
    }
}
