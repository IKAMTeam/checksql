package com.onevizion.checksql.vo;

import java.util.ArrayList;
import java.util.List;

public class Configuration {

    private String remoteOwner;
    //private String remoteUser;
    private String localOwner;
    //private String localUser;
    //private boolean enabledSql = true;
    //private boolean enabledPlSql = true;
    //private List<String> skipTablesSql = new ArrayList<String>();
    //private List<String> skipTablesPlSql = new ArrayList<String>();
    private String owner1DbSchema;
    private String owner2DbSchema;
    private String test1DbSchema;
    private String test2DbSchema;

    private boolean useSecondTest;

    public String getRemoteOwner() {
        return remoteOwner;
    }

    public void setRemoteOwner(String remoteOwner) {
        this.remoteOwner = remoteOwner;
    }

    /*public String getRemoteUser() {
        return remoteUser;
    }*/

    /*public void setRemoteUser(String remoteUser) {
        this.remoteUser = remoteUser;
    }*/

    public String getLocalOwner() {
        return localOwner;
    }

    public void setLocalOwner(String localOwner) {
        this.localOwner = localOwner;
    }

    /*public String getLocalUser() {
        return localUser;
    }*/

    /*public void setLocalUser(String localUser) {
        this.localUser = localUser;
    }*/

    /*public boolean isEnabledSql() {
        return enabledSql;
    }*/

    /*public void setEnabledSql(boolean enabledSql) {
        this.enabledSql = enabledSql;
    }*/

    /*public boolean isEnabledPlSql() {
        return enabledPlSql;
    }*/

    /*public void setEnabledPlSql(boolean enabledPlSql) {
        this.enabledPlSql = enabledPlSql;
    }*/

    /*public List<String> getSkipTablesSql() {
        return skipTablesSql;
    }*/

    /*public void setSkipTablesSql(List<String> skipTablesSql) {
        this.skipTablesSql = skipTablesSql;
    }*/

    /*public List<String> getSkipTablesPlSql() {
        return skipTablesPlSql;
    }*/

    /*public void setSkipTablesPlSql(List<String> skipTablesPlSql) {
        this.skipTablesPlSql = skipTablesPlSql;
    }*/

    public boolean isUseSecondTest() {
        return useSecondTest;
    }

    public void setUseSecondTest(boolean useSecondTest) {
        this.useSecondTest = useSecondTest;
    }

    /*public boolean isSkippedSqlTable(String tableName) {
        return getSkipTablesSql().contains(tableName);
    }*/

    /*public boolean isSkippedPlsqlTable(String tableName) {
        return getSkipTablesPlSql().contains(tableName);
    }*/

    public String getOwner1DbSchema() {
        return owner1DbSchema;
    }

    public void setOwner1DbSchema(String owner1DbSchema) {
        this.owner1DbSchema = owner1DbSchema;
    }

    public String getOwner2DbSchema() {
        return owner2DbSchema;
    }

    public void setOwner2DbSchema(String owner2DbSchema) {
        this.owner2DbSchema = owner2DbSchema;
    }

    public String getTest1DbSchema() {
        return test1DbSchema;
    }

    public void setTest1DbSchema(String test1DbSchema) {
        this.test1DbSchema = test1DbSchema;
    }

    public String getTest2DbSchema() {
        return test2DbSchema;
    }

    public void setTest2DbSchema(String test2DbSchema) {
        this.test2DbSchema = test2DbSchema;
    }

}