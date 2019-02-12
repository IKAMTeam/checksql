package com.onevizion.checksql;

import com.onevizion.checksql.exception.AppStartupException;
import com.onevizion.checksql.vo.Configuration;
import com.onevizion.checksql.vo.TableNode;
import oracle.jdbc.driver.OracleConnection;
import oracle.ucp.jdbc.PoolDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CheckSqlApp {

    protected static Logger logger;

    private static final String DB_CNN_PROPS_ERROR_MESSAGE = "DB connection properties should be specified in following format: <username>/<password>@<host>:<port>:<SID>";

    private static final String JDBC_THIN_URL_PREFIX = "jdbc:oracle:thin:@";
    
    private List<TableNode> values = new ArrayList();

    /**
     * Main use cases:
     * 1 - When RemoteOwner and RemoteUser are set,
     * then get queries and blocks from remote owner and test on remote user
     * (for check queries and blocks in current version)
     * 2 - When RemoteOwner and RemoteUser and LocalOwner and LocalUser are set,
     * then get queries and blocks from remote owner and test on local user
     * (for check queries and blocks in new version)
     * 
     * @param args
     *            - (optional) Path to config file
     */
    public static void main(String[] args) throws Exception {
        CheckSqlApp app = new CheckSqlApp();
        List<String> listArgs;
        //Document doc;
        if (args.length > 0) {
            listArgs = Arrays.asList(args);
        } else {
            throw new Exception("Expected command line arguments: <remote_owner>/<password>@<connect_identifier> [<local_owner>/<password>@<connect_identifier>] [path_to_config_file]");
        }

        Configuration configuration = ConfigurationUtils.loadConfiguration(listArgs);

        ApplicationContext ctx = app.getAppContext("com/onevizion/checksql/beans.xml", configuration);

        String[] owner1DbUrlParts = parseDbCnnStr(configuration.getRemoteOwner());
        configuration.setOwner1DbSchema(owner1DbUrlParts[0]);
        configDataSource((PoolDataSource) ctx.getBean("owner1DataSource"), owner1DbUrlParts, "check-sql_owner1");

        String[] test1DbUrlParts = parseDbCnnStr(configuration.getRemoteOwner());
        configuration.setTest1DbSchema(test1DbUrlParts[0]);
        configDataSource((PoolDataSource) ctx.getBean("test1DataSource"), test1DbUrlParts, "check-sql_test1");

        if (configuration.isUseSecondTest()) {
            String[] owner2DbUrlParts = parseDbCnnStr(configuration.getLocalOwner());
            configuration.setOwner2DbSchema(owner2DbUrlParts[0]);
            configDataSource((PoolDataSource) ctx.getBean("owner2DataSource"), owner2DbUrlParts, "check-sql_owner2");

            //String[] test2DbUrlParts = parseDbCnnStr(configuration.getLocalUser());
            String[] test2DbUrlParts = parseDbCnnStr(configuration.getLocalOwner());
            configuration.setTest2DbSchema(test2DbUrlParts[0]);
            configDataSource((PoolDataSource) ctx.getBean("test2DataSource"), test2DbUrlParts, "check-sql_test2");
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

            logger.error(CheckSqlExecutor.INFO_MARKER, "Exception", e);
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
        /*if (StringUtils.isBlank(configuration.getRemoteOwner()) || StringUtils.isBlank(configuration.getRemoteUser())) {
            throw new IllegalArgumentException("both remote_owner and remote_user should set");
        }*/
        if (StringUtils.isBlank(configuration.getRemoteOwner())) {
            throw new IllegalArgumentException("remote_owner should be set");
        }

        /*if ((StringUtils.isNotBlank(configuration.getLocalOwner()) && StringUtils.isBlank(configuration.getLocalUser()))
                || (StringUtils.isBlank(configuration.getLocalOwner())
                        && StringUtils.isNotBlank(configuration.getLocalUser()))) {
            throw new IllegalArgumentException("both local_owner and local_user should set or nothing");
        }*/

        parseDbCnnStr(configuration.getRemoteOwner());
        //parseDbCnnStr(configuration.getRemoteUser());

        if (configuration.isUseSecondTest()) {
            parseDbCnnStr(configuration.getLocalOwner());
            //parseDbCnnStr(configuration.getLocalUser());
        }
    }

    private ApplicationContext configAppContext(String beansXmlClassPath) {
        ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:" + beansXmlClassPath);
        return ctx;
    }

    private static void configDataSource(PoolDataSource ds, String[] cnnProps, String programName) {
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
        } catch (SQLException e) {
            logger.info(CheckSqlExecutor.INFO_MARKER, "Can't set connection properties", e);
        }
    }

}