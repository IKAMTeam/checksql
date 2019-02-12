package com.onevizion.checksql;

import com.onevizion.checksql.vo.Configuration;

import java.util.List;

public class ConfigurationUtils {

    private static final String CONFIG_FILE_REGEXP = ".*?\\.json";

    public static Configuration loadConfiguration(List<String> listArgs) {
        return loadConfigurationFromFile(listArgs);
    }

    private static Configuration loadConfigurationFromFile(List<String> listArgs) {
        Configuration configuration = new Configuration();

        String remote_owner;
        String local_owner = null;
        String pathToConfigFile = null;

        if (listArgs.size() == 1) {
            remote_owner = listArgs.get(0);
            local_owner = null;
        } else if (listArgs.size() == 2) {
            remote_owner = listArgs.get(0);

            if (listArgs.get(1).matches(CONFIG_FILE_REGEXP)) {
                pathToConfigFile = listArgs.get(1);
            } else {
                local_owner = listArgs.get(1);
            }
        } else {
            remote_owner = listArgs.get(0);

            if (listArgs.get(1).matches(CONFIG_FILE_REGEXP)) {
                pathToConfigFile = listArgs.get(1);
                local_owner = listArgs.get(2);
            } else {
                local_owner = listArgs.get(1);
                pathToConfigFile = listArgs.get(2);
            }
        }
        
        configuration.setRemoteOwner(remote_owner);
        if (local_owner == null) {
            configuration.setLocalOwner(null);
        } else {
            configuration.setLocalOwner(local_owner);
        }

        if (StringUtils.isNotBlank(configuration.getLocalOwner())) {
                configuration.setUseSecondTest(true);
        }

        if (StringUtils.isNotBlank(pathToConfigFile)) {
            configuration.setPathToConfigFile(pathToConfigFile);
        }

        return configuration;
    }

}