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

    private static final String JDBC_THIN_URL_PREFIX = "jdbc:oracle:thin:@";

    private static final String ARGS_ERROR_MESSAGE = "Expected command line arguments: <remote_owner>/<remote_owner_pwd>@<remote_owner_connect_identifier> <remote_user>/<remote_user_pwd>@<remote_user_connect_identifier> [<local_owner>/<local_owner_pwd>@<local_owner_connect_identifier> <local_user>/<local_user_pwd>@<local_user_connect_identifier>]";

    public static void main(String[] args) {
        CheckSqlApp app = new CheckSqlApp();

        ApplicationContext ctx = app.getAppContext("com/onevizion/checksql/beans.xml", args);

        configDataSource((PoolDataSource) ctx.getBean("owner1DataSource"), parseDbCnnStr(args[0]), "check-sql_owner1", false, true);
        configDataSource((PoolDataSource) ctx.getBean("test1DataSource"), parseDbCnnStr(args[1]), "check-sql_test1", false, false);

        if (args.length == 4) {
            configDataSource((PoolDataSource) ctx.getBean("owner2DataSource"), parseDbCnnStr(args[2]), "check-sql_owner2", true, true);
            configDataSource((PoolDataSource) ctx.getBean("test2DataSource"), parseDbCnnStr(args[3]), "check-sql_test2", true, false);
        }

        CheckSqlExecutor executor = ctx.getBean(CheckSqlExecutor.class);
        try {
            executor.run(args.length == 4);
        } catch (Exception e) {
            logger.error("Unexpected error", e);
        }
    }

    private static String[] parseDbCnnStr(String cnnStr) {
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

    private ApplicationContext getAppContext(String beansXmlClassPath, String[] args) {
        try {
            checkArgsAndThrow(args);
            configLogger(args);
            ApplicationContext ctx = configAppContext(beansXmlClassPath, args);
            return ctx;
        } catch (Exception e) {
            if (logger == null) {
                // Exception thrown before logger were instantiated, schema name is unknown
                System.setProperty("schema", "UNKNOWN");
                logger = LoggerFactory.getLogger(this.getClass());
            }
            String ver = getClass().getPackage().getImplementationVersion();
            MDC.put("Subject", ExceptionUtils.getSubj(e, ver));

            logger.error("Exception", e);
            throw new AppStartupException(e);
        }
    }

    private void configLogger(String[] args) {
        String[] cnnProps = parseDbCnnStr(args[0]);

        // System property to be used by logger
        String schema = cnnProps[0] + "." + cnnProps[2].split("@")[1].split("\\.")[0];
        System.setProperty("schema", schema.replace(":", ""));
        logger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    }

    private void checkArgsAndThrow(String[] args) throws IllegalArgumentException {
        if (args.length != 2 && args.length != 4) {
            throw new IllegalArgumentException(ARGS_ERROR_MESSAGE);
        }

        parseDbCnnStr(args[0]);
        parseDbCnnStr(args[1]);

        if (args.length == 4) {
            parseDbCnnStr(args[2]);
            parseDbCnnStr(args[3]);
        }
    }

    private ApplicationContext configAppContext(String beansXmlClassPath, String[] args) {
        ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:" + beansXmlClassPath);
        return ctx;
    }

    private static void configDataSource(PoolDataSource ds, String[] cnnProps, String programName, boolean isLocal, boolean isOwner) {
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
            if (isLocal) {
                if (isOwner) {
                    logger.info("The data source is configured: local_owner=" + ds.getUser() + ", url=" + ds.getURL());
                } else {
                    logger.info("The data source is configured: local_user=" + ds.getUser() + ", url=" + ds.getURL());
                }
            } else {
                if (isOwner) {
                    logger.info("The data source is configured: remote_owner=" + ds.getUser() + ", url=" + ds.getURL());
                } else {
                    logger.info("The data source is configured: remote_user=" + ds.getUser() + ", url=" + ds.getURL());
                }
            }
        } catch (SQLException e) {
            logger.warn("Can't set connection properties", e);
        }
    }

}