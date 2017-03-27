package com.onevizion.checksql.vo;

public interface CheckSqlQuery {

    public String getSqlColName();

    public String getPrimKeyColName();

    public String getTableName();

    public String getSql();

    public String getQueryType();
}
