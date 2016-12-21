package com.onevizion.checksql;

import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jdom2.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.onevizion.checksql.exception.AppStartupException;
import com.onevizion.checksql.vo.Configuration;

import oracle.jdbc.driver.OracleConnection;
import oracle.ucp.jdbc.PoolDataSource;

public class CheckSqlApp {

    protected static Logger logger;

    private static final String DB_CNN_PROPS_ERROR_MESSAGE = "DB connection properties should be specified in following format: <username>/<password>@<host>:<port>:<SID>";

    private static final String JDBC_THIN_URL_PREFIX = "jdbc:oracle:thin:@";

    /**
     * 
     * @param args
     *      - (optional) Path to config file
     */
    public static void main(String[] args) {
        CheckSqlApp app = new CheckSqlApp();

        Document doc;
        if (args.length > 0) {
            doc = XmlConfUtils.getDoc(args[0]);
        } else {
            doc = XmlConfUtils.getDoc("check-sql.xml");
        }
        Configuration configuration = ConfigurationUtils.loadConfiguration(doc);

        ApplicationContext ctx = app.getAppContext("com/onevizion/checksql/beans.xml", configuration);

        configDataSource((PoolDataSource) ctx.getBean("owner1DataSource"),parseDbCnnStr(configuration.getRemoteOwner()), "check-sql_owner1", false, true);
        configDataSource((PoolDataSource) ctx.getBean("test1DataSource"), parseDbCnnStr(configuration.getRemoteUser()), "check-sql_test1", false, false);

        if (configuration.isUseSecondTest()) {
            configDataSource((PoolDataSource) ctx.getBean("owner2DataSource"), parseDbCnnStr(configuration.getLocalOwner()), "check-sql_owner2", true, true);
            configDataSource((PoolDataSource) ctx.getBean("test2DataSource"), parseDbCnnStr(configuration.getLocalUser()), "check-sql_test2", true, false);
        }

        CheckSqlExecutor executor = ctx.getBean(CheckSqlExecutor.class);
        try {
            executor.run(configuration);
        } catch (Exception e) {
            logger.error(CheckSqlExecutor.INFO_MARKER, "Unexpected error", e);
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

    private ApplicationContext getAppContext(String beansXmlClassPath, Configuration configuration) {
        try {
            checkArgsAndThrow(configuration);
            configLogger(configuration);
            ApplicationContext ctx = configAppContext(beansXmlClassPath);
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

    private void configLogger(Configuration configuration) {
        String[] cnnProps = parseDbCnnStr(configuration.getRemoteOwner());

        // System property to be used by logger
        String schema = cnnProps[0] + "." + cnnProps[2].split("@")[1].split("\\.")[0];
        System.setProperty("schema", schema.replace(":", ""));
        logger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    }

    private void checkArgsAndThrow(Configuration configuration) throws IllegalArgumentException {
        if (StringUtils.isBlank(configuration.getRemoteOwner()) || StringUtils.isBlank(configuration.getRemoteUser())) {
            throw new IllegalArgumentException("both remote_owner and remote_user should set");
        }

        if ((StringUtils.isNotBlank(configuration.getLocalOwner()) && StringUtils.isBlank(configuration.getLocalUser()))
                || (StringUtils.isBlank(configuration.getLocalOwner()) && StringUtils.isNotBlank(configuration.getLocalUser()))) {
            throw new IllegalArgumentException("both local_owner and local_user should set or nothing");
        }

        parseDbCnnStr(configuration.getRemoteOwner());
        parseDbCnnStr(configuration.getRemoteUser());

        if (configuration.isUseSecondTest()) {
            parseDbCnnStr(configuration.getLocalOwner());
            parseDbCnnStr(configuration.getLocalUser());
        }
    }

    private ApplicationContext configAppContext(String beansXmlClassPath) {
        ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:" + beansXmlClassPath);
        return ctx;
    }

    private static void configDataSource(PoolDataSource ds, String[] cnnProps, String programName, boolean isLocal,
            boolean isOwner) {
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