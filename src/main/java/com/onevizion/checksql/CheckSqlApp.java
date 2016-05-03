package com.onevizion.checksql;

import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import oracle.jdbc.driver.OracleConnection;
import oracle.ucp.jdbc.PoolDataSource;

public class CheckSqlApp {

    protected static Logger logger;

    private static final String DB_CNN_PROPS_ERROR_MESSAGE = "DB connection properties should be specified in following format: <username>/<password>@<host>:<port>:<SID>";

    public static final String JDBC_THIN_URL_PREFIX = "jdbc:oracle:thin:@";

    private static final String ARGS_ERROR_MESSAGE = "Expected command line arguments: <owner>/<owner_pwd>@<owner_connect_identifier> <test>/<test_pwd>@<test_connect_identifier> <version_mode>";

    public static void main(String[] args) {
        CheckSqlApp app = new CheckSqlApp();

        ApplicationContext ctx = app.getAppContext("com/onevizion/checksql/beans.xml", args);

        String[] ownerCnnProps = parseDbCnnStr(args[0]);
        configDataSource((PoolDataSource) ctx.getBean("ownerDataSource"), ownerCnnProps, "check-sql_owner");

        String[] testCnnProps = parseDbCnnStr(args[1]);
        configDataSource((PoolDataSource) ctx.getBean("testDataSource"), testCnnProps, "check-sql_test");

        Long versionMode = Long.valueOf(args[2]);

        CheckSqlExecutor executor = ctx.getBean(CheckSqlExecutor.class);
        try {
            executor.run(versionMode);
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        }
    }

    public static String[] parseDbCnnStr(String cnnStr) {
        Pattern p = Pattern.compile("(.+?)/(.+?)@(.+)");
        Matcher m = p.matcher(cnnStr);
        String[] props = new String[3];
        if (m.matches() && m.groupCount() == 3) {
            props[0] = m.group(1);
            props[1] = m.group(2);
            props[2] = JDBC_THIN_URL_PREFIX + m.group(3);
        } else {
            throw new IllegalArgumentException(DB_CNN_PROPS_ERROR_MESSAGE);
        }
        return props;
    }

    public ApplicationContext getAppContext(String beansXmlClassPath, String[] args) {
        try {
            checkArgsAndThrow(args);
            configLogger(args);
            ApplicationContext ctx = configAppContext(beansXmlClassPath, args);
            return ctx;

        } catch (Exception e) {
            if (logger == null) {
                // Exception thrown before logger were instantiated, schema name
                // is unknown
                System.setProperty("schema", "UNKNOWN");
                logger = LoggerFactory.getLogger(this.getClass());
            }
            String ver = getClass().getPackage().getImplementationVersion();
            MDC.put("Subject", ExceptionUtils.getSubj(e, ver));

            logger.error("Exception", e);
            throw new AppStartupException(e);
        }
    }

    protected void configLogger(String[] args) {
        String[] cnnProps = parseDbCnnStr(args[0]);

        // System property to be used by logger
        String schema = cnnProps[0] + "." + cnnProps[2].split("@")[1].split("\\.")[0];
        System.setProperty("schema", schema.replace(":", ""));
        logger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    }

    private void checkArgsAndThrow(String[] args) throws IllegalArgumentException {
        if (args.length != 3) {
            throw new IllegalArgumentException(ARGS_ERROR_MESSAGE);
        }
        parseDbCnnStr(args[0]);
        parseDbCnnStr(args[1]);

        if (!"0".equals(args[2]) && !"1".equals(args[2])) {
            throw new IllegalArgumentException(ARGS_ERROR_MESSAGE);
        }
    }

    private ApplicationContext configAppContext(String beansXmlClassPath, String[] args) {
        String[] cnnProps = parseDbCnnStr(args[0]);
        System.setProperty("username", cnnProps[0]);
        System.setProperty("password", cnnProps[1]);
        System.setProperty("url", cnnProps[2]);

        ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:" + beansXmlClassPath);
        return ctx;
    }

    protected static void configDataSource(
            PoolDataSource ds, String[] cnnProps, String programName) {
        try {
            ds.setUser(cnnProps[0]);
            ds.setPassword(cnnProps[1]);
            ds.setURL(cnnProps[2]);

            Properties props;
            props = ds.getConnectionProperties();
            if (props == null) {
                props = new Properties();
            }
            props.setProperty(OracleConnection.CONNECTION_PROPERTY_THIN_VSESSION_PROGRAM, programName);
            ds.setConnectionProperties(props);
            logger.info("The data source is configured: user=" + ds.getUser() + ", url=" + ds.getURL());
        } catch (SQLException e) {
            logger.warn("Can't set connection properties", e);
        }

    }
}
