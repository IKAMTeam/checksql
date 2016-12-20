package com.onevizion.checksql;

public enum PlsqlBlock {

    IMP_DATA_MAP(1, true, "imp_data_map", "imp_data_map", "sql_text", "imp_data_map_id", "length(sql_text) > 0"),
    IMP_DATA_TYPE(2, false, "imp_data_type", "imp_data_type", "sql_text", "imp_data_type_id", null),
    IMP_DATA_ENTITY(3, true, "imp_entity", "imp_entity", "sql_text", "imp_entity_id", null),
    IMP_SPEC(4, true, "imp_spec", "imp_spec", "external_proc", "imp_spec_id", null),
    RULE(5, true, "rule", "rule", "sql_text", "rule_id", "is_enabled = 1"),
    RULE_CLASS_PARAM_VAL(6, true, "rule_class_param_value", "rule_class_param_value v join rule r on (r.rule_id = v.rule_id)", "v.value_clob", "v.rule_class_param_value_id", "r.is_enabled = 1"),
    WF_STEP(7, true, "wf_step", "wf_step s join wf_workflow w on (w.wf_workflow_id = s.wf_workflow_id)", "s.plsql_block", "s.wf_step_id", "w.wf_state_id not in (4,5)"),
    WF_TEMPLATE_STEP(8, true, "wf_template_step", "wf_template_step", "plsql_block", "wf_template_step_id", null);

    public static final String TOTAL_ROWS_COL_NAME = "totalrows";

    private final boolean checkQuery;
    private final String fromClause;
    private final String sqlColName;
    private final String primKeyColName;
    private final String whereClause;
    private final String tableName;
    private final int ordNum;

    private PlsqlBlock(int ordNum, boolean checkQuery, String tableName, String fromClause, String sqlColName,
            String primKeyColName,
            String whereClause) {
        this.ordNum = ordNum;
        this.checkQuery = checkQuery;
        this.fromClause = fromClause;
        this.sqlColName = sqlColName;
        this.primKeyColName = primKeyColName;
        this.whereClause = whereClause;
        this.tableName = tableName;
    }

    public String getSqlColName() {
        return sqlColName;
    }

    public int getOrdNum() {
        return ordNum;
    }

    public String getFromClause() {
        return fromClause;
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
        sql.append(getSqlColName());
        sql.append(", count(*) over () as ");
        sql.append(TOTAL_ROWS_COL_NAME);
        sql.append(" from ");
        sql.append(getFromClause());
        sql.append(" where ");
        sql.append(getSqlColName());
        sql.append(" is not null");
        if (getWhereClause() != null) {
            sql.append(" and ");
            sql.append(getWhereClause());
        }
        return sql.toString();
    }
}
