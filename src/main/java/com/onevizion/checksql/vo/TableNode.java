package com.onevizion.checksql.vo;

public class TableNode implements CheckSqlQuery{
    
	public final String TOTAL_ROWS_COL_NAME;
    
    private final String fromClause;
    private final String sqlColName;
    private final String primKeyColName;
    private final String whereClause;
    private final String tableName;
    private final String queryType;
    private final int ordNum;
    
    TableNode(int ordNum, String tableName, String fromClause, String sqlColName,
	            String primKeyColName, String whereClause, String queryType, String TOTAL_ROWS_COL_NAME) {
	    this.ordNum = ordNum;
	    this.fromClause = fromClause;
	    this.sqlColName = sqlColName;
	    this.primKeyColName = primKeyColName;
	    this.whereClause = whereClause;
	    this.tableName = tableName;   
	    this.queryType = queryType;
	    this.TOTAL_ROWS_COL_NAME = TOTAL_ROWS_COL_NAME;
    }
    
    @Override
    public String getSqlColName() {
        return sqlColName;
    }

    public int getOrdNum() {
        return ordNum;
    }

    public String getFromClause() {
        return fromClause;
    }

    @Override
    public String getPrimKeyColName() {
        return primKeyColName;
    }

    public String getWhereClause() {
        return whereClause;
    }

    @Override
    public String getTableName() {
        return tableName;
    }
    
    @Override
    public String getQueryType() {
        return queryType;
    }
    
    @Override
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