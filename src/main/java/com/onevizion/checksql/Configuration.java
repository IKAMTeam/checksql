package com.onevizion.checksql;

import java.util.List;

public class Configuration {

    private String remoteOwner;
    private String remoteUser;
    private String localOwner;
    private String localUser;
    private boolean enabledSql;
    private boolean enabledPlSql;
    private List<String> skipTablesSql;
    private List<String> skipTablesPlSql;

    private boolean useSecondTest;

    public String getRemoteOwner() {
        return remoteOwner;
    }

    public void setRemoteOwner(String remoteOwner) {
        this.remoteOwner = remoteOwner;
    }

    public String getRemoteUser() {
        return remoteUser;
    }

    public void setRemoteUser(String remoteUser) {
        this.remoteUser = remoteUser;
    }

    public String getLocalOwner() {
        return localOwner;
    }

    public void setLocalOwner(String localOwner) {
        this.localOwner = localOwner;
    }

    public String getLocalUser() {
        return localUser;
    }

    public void setLocalUser(String localUser) {
        this.localUser = localUser;
    }

    public boolean isEnabledSql() {
        return enabledSql;
    }

    public void setEnabledSql(boolean enabledSql) {
        this.enabledSql = enabledSql;
    }

    public boolean isEnabledPlSql() {
        return enabledPlSql;
    }

    public void setEnabledPlSql(boolean enabledPlSql) {
        this.enabledPlSql = enabledPlSql;
    }

    public List<String> getSkipTablesSql() {
        return skipTablesSql;
    }

    public void setSkipTablesSql(List<String> skipTablesSql) {
        this.skipTablesSql = skipTablesSql;
    }

    public List<String> getSkipTablesPlSql() {
        return skipTablesPlSql;
    }

    public void setSkipTablesPlSql(List<String> skipTablesPlSql) {
        this.skipTablesPlSql = skipTablesPlSql;
    }

    public boolean isUseSecondTest() {
        return useSecondTest;
    }

    public void setUseSecondTest(boolean useSecondTest) {
        this.useSecondTest = useSecondTest;
    }

}