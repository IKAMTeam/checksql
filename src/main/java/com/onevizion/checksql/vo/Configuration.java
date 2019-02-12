package com.onevizion.checksql.vo;

public class Configuration {

    private String remoteOwner;
    private String localOwner;
    private String owner1DbSchema;
    private String owner2DbSchema;
    private String test1DbSchema;
    private String test2DbSchema;
    private String pathToConfigFile;
    private boolean useSecondTest;

    public String getRemoteOwner() {
        return remoteOwner;
    }

    public void setRemoteOwner(String remoteOwner) {
        this.remoteOwner = remoteOwner;
    }

    public String getLocalOwner() {
        return localOwner;
    }

    public void setLocalOwner(String localOwner) {
        this.localOwner = localOwner;
    }

    public boolean isUseSecondTest() {
        return useSecondTest;
    }

    public void setUseSecondTest(boolean useSecondTest) {
        this.useSecondTest = useSecondTest;
    }

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

    public String getPathToConfigFile() {
        return pathToConfigFile;
    }

    public void setPathToConfigFile(String pathToConfigFile) {
        this.pathToConfigFile = pathToConfigFile;
    }

}