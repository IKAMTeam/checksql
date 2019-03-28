package com.onevizion.checksql;

import com.onevizion.checksql.exception.SqlParsingException;
import com.onevizion.checksql.exception.UnexpectedException;
import com.onevizion.checksql.vo.*;
import oracle.ucp.jdbc.PoolDataSourceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class CheckSqlExecutor {

    @Resource(name = "owner1JdbcTemplate")
    private JdbcTemplate owner1JdbcTemplate;

    @Resource(name = "owner2JdbcTemplate")
    private JdbcTemplate owner2JdbcTemplate;

    @Resource(name = "test1JdbcTemplate")
    private JdbcTemplate test1JdbcTemplate;

    @Resource(name = "test1NamedParamJdbcTemplate")
    private NamedParameterJdbcTemplate test1NamedParamJdbcTemplate;

    @Resource(name = "test2JdbcTemplate")
    private JdbcTemplate test2JdbcTemplate;

    @Resource(name = "test2NamedParamJdbcTemplate")
    private NamedParameterJdbcTemplate test2NamedParamJdbcTemplate;

    @Resource
    private AppSettings appSettings;

    private static final String FIND_IMP_DATA_TYPE_PARAM_SQL_PARAM_BY_IMP_DATA_TYPE_ID = "select sql_parameter from imp_data_type_param where imp_data_type_id = ?";

    private static final String FIND_RULE_PARAM_SQL_PARAM_BY_ENTITY_ID = "select ID_FIELD from rule r join rule_type t on (r.rule_type_id = t.rule_type_id) where r.rule_id = ?";

    private static final String FIND_IMP_ENTITY_PARAM_SQL_PARAM_BY_ENTITY_ID = "select sql_parameter from imp_entity_param where imp_entity_id = ?";

    private static final String FIND_DB_OBJECT_ERRORS = "select text from all_errors where name = ? and type = 'PROCEDURE'";

    private static final String PLSQL_PROC_NAME = "CHECKSQL_PLSQL";

    private static final String SELECT_VIEW_NAME = "CHECKSQL_SELECT";

    private static final String DROP_PLSQL_PROC = "drop procedure " + PLSQL_PROC_NAME;

    private static final String DROP_SELECT_VIEW = "drop view " + SELECT_VIEW_NAME;

    private static final String VALUE_BIND_VAR = ":VALUE";
    private static final String VALUE_BIND_VAR_RSTR = ":RETURN_STR";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public static final Marker INFO_MARKER = MarkerFactory.getMarker("INFO_SQL");

    private static final Marker DATA_MARKER = MarkerFactory.getMarker("DATA_SQL");

    public static final Marker ERR_MARKER = MarkerFactory.getMarker("ERR_SQL");

    private static SelectQuery selectQuery;

    private static final String FIND_FIRST_PROGRAM_ID_NEW = "select program_id from program where rownum < 2 and program_id <> 0";

    private static final String FIND_FIRST_PROGRAM_ID_OLD = "select program_id from v_program where rownum < 2";

    public static final String LINE_DELIMITER = "\r\n";

    private static final String ERROR_MSG = "Invalid value in {}.{} where {} = {}" + LINE_DELIMITER + "{}" + LINE_DELIMITER;
    private static final String START_MSG = "checksql {} for schema [{}]" + LINE_DELIMITER;
    private static final String JDBC_THIN_URL_PREFIX = "jdbc:oracle:thin:@";
    private static final String SUMMARY_MSG = "checksql Summary for schema [{}]";
    private static final String BIND_VAR_AND_ASSIGNMENT_OPERATOR_REGEXP = ":((?!:=|;).)*:=";
    private static final String BIND_VAR_REPLACEMENT = "v_ret_str :=";
    private static final String BIND_VAR_REPLACEMENT_IMP_ENTITY = "v_imp_entity :=";


    private List<SqlError> sqlErrors;
    private List<SqlError> configErrors;

    private boolean dropView;
    private boolean dropProc;
    private Configuration config;

    private HashMap<String, Long> tableStats = new HashMap<>();

    public CheckSqlExecutor() {
        sqlErrors = new ArrayList<>();
        configErrors = new ArrayList<>();
    }

    public void run(Configuration config) {
        logger.info(INFO_MARKER, START_MSG, getClass().getPackage().getImplementationVersion(),
                parseUrlToSchemaWithUrlBeforeDot(config.getOwner1DbSchema(), config.getUrl()));
        this.config = config;

        configAppSettings();
        try {
            if (StringUtils.isBlank(config.getPathToConfigFile())) {
                selectQuery = new SelectQuery(owner1JdbcTemplate);
            } else {
                selectQuery = new SelectQuery(owner1JdbcTemplate, config.getPathToConfigFile());
            }
        } catch (Exception e) {
            logger.info(INFO_MARKER, "checksql is failed with error\r\n{}", e);
            return;
        }

        try {
            testSelectAndPlsqlBlockForAllTables();
        } catch (Exception e) {
            logger.info(INFO_MARKER, "checksql is failed with error\r\n{}", e);
            return;
        }
        logTableStats();
        logger.info(INFO_MARKER, "checksql completed");
    }

    private void configAppSettings() {
        appSettings.setTest1Pid(getRandomTest1Pid());

        PoolDataSourceImpl test1DataSource = (PoolDataSourceImpl) test1JdbcTemplate.getDataSource();
        appSettings.setTest1Schema(test1DataSource.getUser());

        if (config.isUseSecondTest()) {
            appSettings.setTest2Pid(getRandomTest2Pid());

            PoolDataSourceImpl test2DataSource = (PoolDataSourceImpl) test2JdbcTemplate.getDataSource();
            appSettings.setTest2Schema(test2DataSource.getUser());
        }
    }

    private Long getRandomTest1Pid() {
        Long pid = null;
        Exception e = null;
        try {
            pid = owner1JdbcTemplate.queryForObject(FIND_FIRST_PROGRAM_ID_OLD, Long.class);         
        } catch (DataAccessException e1) {
            e = e1;
        }

        if (pid == null) {
            try {
                pid = owner1JdbcTemplate.queryForObject(FIND_FIRST_PROGRAM_ID_NEW, Long.class);
            } catch (DataAccessException e1) {
                e = e1;
            }
        }

        if (pid == null) {
            throw new UnexpectedException("[Test1] Can not get a PROGRAM_ID", e);
        }
        return pid;
    }

    private Long getRandomTest2Pid() {
        Long pid = null;
        Exception e = null;
        try {
            pid = owner2JdbcTemplate.queryForObject(FIND_FIRST_PROGRAM_ID_OLD, Long.class);
        } catch (DataAccessException e1) {
            e = e1;
        }

        if (pid == null) {
            try {
                pid = owner2JdbcTemplate.queryForObject(FIND_FIRST_PROGRAM_ID_NEW, Long.class);
            } catch (DataAccessException e1) {
                e = e1;
            }
        }

        if (pid == null) {
            throw new UnexpectedException("[Test2] Can not get a PROGRAM_ID", e);
        }
        return pid;
    }

    private void logTableStats() {
        SortedMap<String, Integer> tableErrStats = new TreeMap<String, Integer>(new Comparator<String>() {

            @Override
            public int compare(String arg0, String arg1) {
                return arg0.compareToIgnoreCase(arg1);
            }
        });
  
        for (TableNode select : selectQuery.values()) {
            tableErrStats.put(select.getTableName().toLowerCase(), 0);
        }

        for (TableNode plsql : /*plsqlBlock*/selectQuery.values()) {
            tableErrStats.put(plsql.getTableName().toLowerCase(), 0);
        }

        for (SqlError err : sqlErrors) {
            if (StringUtils.isBlank(err.getTableName())) {
                continue;
            }
            String tableName = err.getTableName().toLowerCase();
            Integer count = tableErrStats.get(tableName);
            count++;
            tableErrStats.put(tableName, count);
        }
        logger.info(INFO_MARKER, SUMMARY_MSG,
                parseUrlToSchemaWithUrlBeforeDot(this.config.getOwner1DbSchema(), this.config.getUrl()));

        logger.info(INFO_MARKER, "Passed (table name, rows checked):");
        for (String tableName : tableErrStats.keySet()) {
            Long passedRowCount = tableStats.get(tableName) - tableErrStats.get(tableName);
            logger.info(INFO_MARKER, tableName + ", " + passedRowCount.toString());
        }

        logger.error(ERR_MARKER, LINE_DELIMITER + "Failed (table name, failures count):");
        for (String tableName : tableErrStats.keySet()) {
            Integer cnt = tableErrStats.get(tableName);
            if(cnt>0) {
                logger.error(ERR_MARKER, tableName + ", " + cnt);
            }
        }

        if (!configErrors.isEmpty()) {
            logger.error(ERR_MARKER, LINE_DELIMITER + "Invalid Configuration:");
        }
        for (SqlError err : configErrors) {
            logger.error(ERR_MARKER, err.getTableName() + "." + err.getSqlColName());
        }
        logger.info(INFO_MARKER, "");
    }

    private TableValue<Boolean> isSelectStatement(Configuration config, String selectSql) {
        String viewDdl = wrapSelectAsView(selectSql);

        boolean viewCreated = false;
        SqlError sqlErr = null;
        if (config.isUseSecondTest()) {
            try {
                test2JdbcTemplate.update(viewDdl);
                viewCreated = true;
            } catch (BadSqlGrammarException e) {
                sqlErr = new SqlError("CREATE-VIEW2");
                sqlErr.setErrMsg(e.getMessage());
                if (e.getSQLException().getCause() != null) {
                    sqlErr.setShortErrMsg(e.getSQLException().getCause().getMessage());
                } else {
                    sqlErr.setShortErrMsg(e.getSQLException().toString());
                }
            } catch (DataAccessException e2) {
                sqlErr = new SqlError("CREATE-VIEW2");
                sqlErr.setErrMsg(e2.getMessage());
            }
        } else {
            try {
                test1JdbcTemplate.update(viewDdl);
                viewCreated = true;
            } catch (BadSqlGrammarException e) {
                sqlErr = new SqlError("CREATE-VIEW1");
                sqlErr.setErrMsg(e.getMessage());
                if (e.getSQLException().getCause() != null) {
                    sqlErr.setShortErrMsg(e.getSQLException().getCause().getMessage());
                } else {
                    sqlErr.setShortErrMsg(e.getSQLException().toString());
                }

            } catch (DataAccessException e) {
                sqlErr = new SqlError("CREATE-VIEW1");
                sqlErr.setErrMsg(e.getMessage());
            }
        }
        if (viewCreated) {
            StringBuilder sbErrors = new StringBuilder();
            if (config.isUseSecondTest()) {
                SqlRowSet errSqlRowSet = test2JdbcTemplate.queryForRowSet(FIND_DB_OBJECT_ERRORS, SELECT_VIEW_NAME);
                while (errSqlRowSet.next()) {
                    TableValue<String> errResult = TableValue.createString(errSqlRowSet, "text");
                    if (errResult.hasError()) {
                        sbErrors.append(errResult.getSqlError().getErrMsg())
                                .append(LINE_DELIMITER);
                        viewCreated = false;
                    } else {
                        sbErrors.append(errResult.getValue())
                                .append(LINE_DELIMITER);
                        viewCreated = false;
                    }
                }
            } else {
                SqlRowSet errSqlRowSet = test1JdbcTemplate.queryForRowSet(FIND_DB_OBJECT_ERRORS, SELECT_VIEW_NAME);
                while (errSqlRowSet.next()) {
                    TableValue<String> errResult = TableValue.createString(errSqlRowSet, "text");
                    if (errResult.hasError()) {
                        sbErrors.append(errResult.getSqlError().getErrMsg())
                                .append(LINE_DELIMITER);
                        viewCreated = false;
                    } else {
                        sbErrors.append(errResult.getValue())
                                .append(LINE_DELIMITER);
                        viewCreated = false;
                    }
                }
            }
            if(!viewCreated) {
                sqlErr = new SqlError("VIEW-ERR");
                sqlErr.setErrMsg(sbErrors.toString().trim());
            }
        }
        return new TableValue<Boolean>(viewCreated, sqlErr);
    }

    private void logFullSqlError(SqlError sqlError) {
        logger.info(DATA_MARKER, "{}", sqlError.toString());
    }

    private void logSqlError(SqlError sqlError) {
        if (ChecksqlErrorType.DEFAULT.equals(sqlError.getChecksqlErrorType())) {
            sqlErrors.add(sqlError);

            logFullSqlError(sqlError);
            logShortError(sqlError.getTableName(), sqlError.getSqlColName(), sqlError.getEntityIdColName(),
                    sqlError.getEntityId(), sqlError.getShortErrMsg());
        } else {
            configErrors.add(sqlError);
        }
    }

    private TableValue<Boolean> isPlsqlBlock(Configuration config, String plsqlBlock, String tableName) {
        String procDdl = wrapBlockAsProc(plsqlBlock, tableName);

        boolean procCreated = false;
        SqlError sqlErr = null;
        if (config.isUseSecondTest()) {
            try {
                test2JdbcTemplate.update(procDdl);
                procCreated = true;
            } catch (BadSqlGrammarException e) {
                sqlErr = new SqlError("CREATE-PROC2");
                if (e.getSQLException().getCause() != null) {
                    sqlErr.setShortErrMsg(e.getSQLException().getCause().getMessage());
                } else {
                    sqlErr.setShortErrMsg(e.getSQLException().toString());
                }
                sqlErr.setErrMsg(e.getMessage());
            } catch (DataAccessException e2) {
                sqlErr = new SqlError("CREATE-PROC2");
                sqlErr.setErrMsg(e2.getMessage());
            }
        } else {
            try {
                test1JdbcTemplate.update(procDdl);
                procCreated = true;
            } catch (BadSqlGrammarException e) {
                sqlErr = new SqlError("CREATE-PROC1");
                if (e.getSQLException().getCause() != null) {
                    sqlErr.setShortErrMsg(e.getSQLException().getCause().getMessage());
                } else {
                    sqlErr.setShortErrMsg(e.getSQLException().toString());
                }
                sqlErr.setErrMsg(e.getMessage());
            } catch (DataAccessException e) {
                sqlErr = new SqlError("CREATE-PROC1");
                sqlErr.setErrMsg(e.getMessage());
            }
        }


        if (procCreated) {
            StringBuilder sbErrors = new StringBuilder();
            if (config.isUseSecondTest()) {
                SqlRowSet errSqlRowSet = test2JdbcTemplate.queryForRowSet(FIND_DB_OBJECT_ERRORS, PLSQL_PROC_NAME);
                while (errSqlRowSet.next()) {
                    TableValue<String> errResult = TableValue.createString(errSqlRowSet, "text");
                    if (errResult.hasError()) {
                        sbErrors.append(errResult.getSqlError().getErrMsg())
                                .append(LINE_DELIMITER);
                        procCreated = false;
                    } else {
                        sbErrors.append(errResult.getValue())
                                .append(LINE_DELIMITER);
                        procCreated = false;
                    }
                }

            } else {
                SqlRowSet errSqlRowSet = test1JdbcTemplate.queryForRowSet(FIND_DB_OBJECT_ERRORS, PLSQL_PROC_NAME);
                while (errSqlRowSet.next()) {
                    TableValue<String> errResult = TableValue.createString(errSqlRowSet, "text");
                    if (errResult.hasError()) {
                        sbErrors.append(errResult.getSqlError().getErrMsg())
                                .append(LINE_DELIMITER);
                        procCreated = false;
                    } else {
                        sbErrors.append(errResult.getValue())
                                .append(LINE_DELIMITER);
                        procCreated = false;
                    }
                }
            }
            if(!procCreated) {
                sqlErr = new SqlError("PROC-ERR");
                sqlErr.setErrMsg(sbErrors.toString().trim());
            }
        }
        return new TableValue<Boolean>(procCreated, sqlErr);
    }

    private boolean isSelectStatement(String statement) {
        if (StringUtils.isBlank(statement)) {
            return false;
        }
        String str = new String(statement);
        str = str.trim().toLowerCase();
        return str.startsWith("select");
    }

    private String removeRowWithValueBindVarIfNeed(String plsql) {
        if (plsql == null || !plsql.contains(VALUE_BIND_VAR)) {
            return plsql;
        }
        String lines[] = plsql.split("\\r?\\n");
        StringBuilder outPlsql = new StringBuilder();
        for (String line : lines) {
            if (!line.toUpperCase().trim().startsWith(":VALUE")) {
                outPlsql.append(line);
                outPlsql.append(System.getProperty("line.separator"));
            }
        }

        return outPlsql.toString();
    }

    private String replaceBindVars(String sql, String tableName, String sqlColName, String entityId) {
        if ("imp_data_type".equalsIgnoreCase(tableName)) {
            sql = replaceImpDataTypeParamByImpDataTypeId(sql, entityId);
            sql = replaceStaticImpDataTypeParam(sql);
        } else if ("imp_entity".equalsIgnoreCase(tableName)) {
            sql = replaceImpEntityVars(sql);
            sql = replaceImpEntityParamsByEntityId(sql, entityId);
        } else if ("imp_spec".equalsIgnoreCase(tableName)) {
            sql = replaceImpSpecExtProcParams(sql);
        } else if ("rule".equalsIgnoreCase(tableName) && "sql_text".equalsIgnoreCase(sqlColName)) {
            sql = replaceRuleParams(sql, entityId);
        } else if ("wf_template_step".equalsIgnoreCase(tableName) || "wf_step".equalsIgnoreCase(tableName)) {
            sql = replaceWfStepParams(sql);
        } else if ("imp_data_map".equalsIgnoreCase(tableName)) {
            sql = replaceImpDataMapVars(sql);
        } else if ("config_field".equalsIgnoreCase(tableName)) {
            sql = replaceConfigFieldVars(sql);
        }

        sql = replaceDateBindVars(sql);
        sql = replaceNonDateBindVars(sql);
        return sql;
    }

    private String replaceImpEntityVars(String sql) {
        sql = sql.replaceAll(VALUE_BIND_VAR, "v_imp_entity");
        sql = sql.replaceAll(VALUE_BIND_VAR.toLowerCase(), "v_imp_entity");
        return sql;
    }

    private String replaceConfigFieldVars(String sql) {
        sql = sql.replaceAll(VALUE_BIND_VAR_RSTR, "v_ret_str");
        sql = sql.replaceAll(VALUE_BIND_VAR_RSTR.toLowerCase(), "v_ret_str");
        return sql;
    }

    private String replaceImpDataMapVars(String sql) {
        sql = sql.replaceAll(VALUE_BIND_VAR, "v_ret_str");
        sql = sql.replaceAll(VALUE_BIND_VAR.toLowerCase(), "v_ret_str");
        return sql;
    }

    private String replaceBindVarsWithAssignmentOperator(String sql, String tableName) {
        if ("imp_entity".equalsIgnoreCase(tableName)) {
            return sql.replaceAll(BIND_VAR_AND_ASSIGNMENT_OPERATOR_REGEXP, BIND_VAR_REPLACEMENT_IMP_ENTITY);
        } else {
            return sql.replaceAll(BIND_VAR_AND_ASSIGNMENT_OPERATOR_REGEXP, BIND_VAR_REPLACEMENT);
        }
    }

    private String replaceWfStepParams(String sql) {
        String newSql = new String(sql);
        newSql = newSql.replaceAll("(?i)" + Pattern.quote(":wf_workflow_id"), "0");
        newSql = newSql.replaceAll("(?i)" + Pattern.quote(":key"), "0");
        newSql = newSql.replaceAll("(?i)" + Pattern.quote(":subkey"), "0");
        newSql = newSql.replaceAll("(?i)" + Pattern.quote(":wpkey"), "0");
        return newSql;
    }

    private String replaceRuleParams(String sql, String entityId) {
        List<String> params = owner1JdbcTemplate.queryForList(FIND_RULE_PARAM_SQL_PARAM_BY_ENTITY_ID,
                String.class, entityId);
        String newSql = new String(sql);
        for (String param : params) {
            if (StringUtils.isNotBlank(param)) {
                if (param.startsWith(":")) {
                    newSql = newSql.replaceAll("(?i)" + Pattern.quote(param), "0");
                } else {
                    newSql = newSql.replaceAll("(?i)" + Pattern.quote(":" + param), "0");
                }
            }
        }
        newSql = newSql.replaceAll("(?i)" + Pattern.quote(":return_str"), "v_ret_str");
        newSql = newSql.replaceAll("(?i)" + Pattern.quote(":id_num"), "0");
        newSql = newSql.replaceAll("(?i)" + Pattern.quote(":pk"), "0");
        newSql = newSql.replaceAll("(?i)" + Pattern.quote(":ln"), "0");
        newSql = newSql.replaceAll("(?i)" + Pattern.quote(":parent_id"), "0");
        newSql = newSql.replaceAll("(?i)" + Pattern.quote(":child_id"), "0");
        newSql = newSql.replaceAll("(?i)" + Pattern.quote(":imp_run_id"), "0");
        return newSql;
    }

    private String replaceImpSpecExtProcParams(String sql) {
        List<String> params = SqlParser.getParams(sql);
        String newSql = new String(sql);
        for (String param : params) {
            newSql = newSql.replaceAll(":" + param, "0");
        }
        return newSql;
    }

    private String replaceImpEntityParamsByEntityId(String sql, String entityId) {
        List<String> sqlParams = owner1JdbcTemplate.queryForList(FIND_IMP_ENTITY_PARAM_SQL_PARAM_BY_ENTITY_ID,
                String.class, entityId);
        String newSql = new String(sql);
        for (String sqlParam : sqlParams) {
            if (StringUtils.isNotBlank(sqlParam)) {
                newSql = newSql.replaceAll(":" + sqlParam, "0");
            }
        }
        return newSql;
    }

    private String replaceImpDataTypeParamByImpDataTypeId(String sql, String impDataTypeId) {
        List<String> sqlParams = owner1JdbcTemplate.queryForList(
                FIND_IMP_DATA_TYPE_PARAM_SQL_PARAM_BY_IMP_DATA_TYPE_ID,
                String.class, impDataTypeId);
        String newSql = new String(sql);
        for (String sqlParam : sqlParams) {
            if (StringUtils.isNotBlank(sqlParam)) {
                if ("V_TABLE".equals(sqlParam) || "TABLE_NAME".equals(sqlParam)) {
                    newSql = newSql.replaceAll(":" + sqlParam, "xitor");
                } else if ("V_KEY_FIELD".equals(sqlParam) || "V_DISPLAY_FIELD".equals(sqlParam) || "KEY_FIELD".equals(sqlParam)) {
                    newSql = newSql.replaceAll(":" + sqlParam, "xitor_id");
                } else if ("COLUMN_NAME".equals(sqlParam)) {
                    newSql = newSql.replaceAll(":" + sqlParam, "xitor_key");
                } else {
                    newSql = newSql.replaceAll(":" + sqlParam, "0");
                }
            }
        }
        return newSql;
    }

    private String wrapBeginEndIfNeed(String entityBlock) {
        String str = new String(entityBlock);
        str = str.trim().toLowerCase();
        boolean wrapBlock = false;
        if (isPlsqlBlock(str)) {
            wrapBlock = !str.startsWith("declare") && !str.startsWith("begin");
        } else {
            wrapBlock = true;
        }
        if (wrapBlock) {
            return "begin\r\n" + entityBlock + "\r\nend;";
        } else {
            return entityBlock;
        }
    }

    private boolean isPlsqlBlock(String val) {
        return !isSelectStatement(val);
    }

    private String wrapBlockAsProc(String entityBlock, String tableName) {
        StringBuilder ddl = new StringBuilder("create or replace procedure ");
        ddl.append(PLSQL_PROC_NAME);
        if ("imp_entity".equalsIgnoreCase(tableName)) {
            ddl.append(" as\r\n v_imp_entity list_id;\r\nbegin\r\n");
        } else {
            ddl.append(" as\r\n v_ret_str varchar2(1000);\r\nbegin\r\n");
        }
        ddl.append(entityBlock);
        ddl.append("\r\nend ");
        ddl.append(PLSQL_PROC_NAME);
        ddl.append(";");
        return ddl.toString();
    }

    private String wrapSelectAsView(String selectQuery) {
        StringBuilder ddl = new StringBuilder("create or replace view ");
        ddl.append(SELECT_VIEW_NAME);
        ddl.append(" as\r\n ");
        ddl.append(selectQuery);
        return ddl.toString();
    }

    private String replaceDateBindVars(String sql) {
        Pattern p = Pattern.compile("to_date[(]{1}\\s*:\\w*\\s*,\\s*'[my]{2}[/.]{1}[dm]{2}[/.][y]{2,4}'[)]{1}");
        Matcher m = p.matcher(sql.toLowerCase());
        if (m.find()) {
            sql = m.replaceAll("to_date('01/01/1990','MM/DD/YYYY')");
        }
        return sql;
    }

    private String replaceNonDateBindVars(String sql) {
        return SqlParser.replaceBindVars(sql, "0");
    }

    private String removeSemicolonAtTheEnd(String sql) {
        String newSql = new String(sql.trim());
        if (newSql.endsWith(";")) {
            newSql = newSql.substring(0, newSql.length() - 1);
        }
        return newSql;
    }

    private String replaceStaticImpDataTypeParam(String sql) {
        sql = sql.replaceAll(":ENTITY_PK", "0");
        sql = sql.replaceAll(":VALUE", "0");
        sql = sql.replaceAll(":\\[USER_ID\\]", "0");
        sql = sql.replaceAll(":\\[PROGRAM_ID\\]", "0");
        sql = sql.replaceAll("\\[DATE_FORMAT\\]", "p");
        sql = sql.replaceAll("\\[COLUMN_NAME\\]", "p");
        sql = sql.replaceAll(":TABLE_NAME", "xitor");

        sql = sql.replaceAll(":\\[ENTITY_ID\\]", "0");
        sql = sql.replaceAll("\\[USER_ID\\]", "0");
        sql = sql.replaceAll("\\[PROGRAM_ID\\]", "0");
        return sql;
    }

    private TableValue<SqlRowSet> getSqlRowSetData(CheckSqlQuery query) {
        SqlRowSet sqlRowSet = null;
        SqlError sqlErr = null;
        try {
            sqlRowSet = owner1JdbcTemplate.queryForRowSet(query.getSql());
        } catch (BadSqlGrammarException e) {
            sqlRowSet = null;
            sqlErr = new SqlError(query.getQueryType() + "-ENTITY", ChecksqlErrorType.CONFIG);
            if (e.getSQLException().getCause() != null) {
                sqlErr.setShortErrMsg(e.getSQLException().getCause().getMessage());
            } else {
                sqlErr.setShortErrMsg(e.getSQLException().toString());
            }
            sqlErr.setErrMsg(e.getMessage());
            sqlErr.setTableName(query.getTableName());
            sqlErr.setSqlColName(query.getSqlColName());
            sqlErr.setEntityIdColName(query.getPrimKeyColName());
            sqlErr.setQuery(query.getSql());
        } catch (DataAccessException e1) {
            sqlRowSet = null;
            sqlErr = new SqlError(query.getQueryType() + "-ENTITY", ChecksqlErrorType.CONFIG);
            sqlErr.setErrMsg(e1.getMessage());
            sqlErr.setTableName(query.getTableName());
            sqlErr.setSqlColName(query.getSqlColName());
            sqlErr.setEntityIdColName(query.getPrimKeyColName());
            sqlErr.setQuery(query.getSql());
        }
        return new TableValue<SqlRowSet>(sqlRowSet, sqlErr);
    }

    private SqlError testSelectStatementPart(SqlRowSet value, TableNode sel) throws Exception {

        TableValue<String> entityId = TableValue.createString(value, sel.getPrimKeyColName());
        TableValue<String> entitySql = TableValue.createString(value, sel.getSqlColName());

        // Remove unavailable statements of SELECT
        String selectSql = new String(entitySql.getValue());

        try {
            if (selectQuery.valueByName("IMP_ENTITY").getTableName().equalsIgnoreCase(sel.getTableName())
                    && isPlsqlBlock(selectSql)) {
                return null;
            }
        } catch (Exception e) {
            selectSql = new String(entitySql.getValue());
        }

        try {
            if (selectQuery.valueByName("IMP_DATA_TYPE_PARAM").getTableName().equalsIgnoreCase(sel.getTableName())) {
                selectSql = replaceStaticImpDataTypeParam(selectSql);
            }
        } catch (Exception e) {
            selectSql = new String(entitySql.getValue());
        }

        if (selectSql.contains("?")) {
            selectSql = selectSql.replace("?", ":p");
        }
        try {
            selectSql = SqlParser.removeIntoClause(selectSql);
        } catch (Exception e) {
            SqlError sqlErr = new SqlError("SELECT-INTO-CLAUSE");
            sqlErr.setErrMsg(e.getMessage());
            sqlErr.setTableName(sel.getTableName());
            sqlErr.setEntityIdColName(sel.getPrimKeyColName());
            sqlErr.setSqlColName(sel.getSqlColName());
            sqlErr.setEntityId(entityId.getValue());
            sqlErr.setTable(sel.getOrdNum());
            sqlErr.setRow(value.getRow());

            return sqlErr;
        }

        selectSql = removeSemicolonAtTheEnd(selectSql);
        selectSql = replaceDateBindVars(selectSql);
        try {
            selectSql = replaceNonDateBindVars(selectSql);
        } catch (SqlParsingException e) {
            SqlError sqlErr = new SqlError("REPLACE-BIND-VARS");
            sqlErr.setTableName(sel.getTableName());
            sqlErr.setEntityIdColName(sel.getPrimKeyColName());
            sqlErr.setSqlColName(sel.getSqlColName());
            sqlErr.setEntityId(entityId.getValue());
            sqlErr.setQuery(selectSql);
            sqlErr.setOriginalQuery(entitySql.getValue());
            sqlErr.setTable(sel.getOrdNum());
            sqlErr.setRow(value.getRow());
            sqlErr.setErrMsg("Can not parse a SELECT to replace bind variables\r\n" + selectSql);
            sqlErr.setShortErrMsg("Can not parse a SELECT to replace bind variables");

            return sqlErr;
        }

        selectSql = "select 1 as val from (\r\n" + selectSql + "\r\n)";
        // Check if a query is Select statement and there are privs with help of creating Oracle View. If view
        // is created then it is Select statement or there are unhandled errors
        TableValue<Boolean> isSelectResult = isSelectStatement(config, selectSql);
        dropView = true;
        if (isSelectResult.hasError()) {
            isSelectResult.getSqlError().setTableName(sel.getTableName());
            isSelectResult.getSqlError().setEntityIdColName(sel.getPrimKeyColName());
            isSelectResult.getSqlError().setSqlColName(sel.getSqlColName());
            isSelectResult.getSqlError().setEntityId(entityId.getValue());
            isSelectResult.getSqlError().setQuery(selectSql);
            isSelectResult.getSqlError().setOriginalQuery(entitySql.getValue());
            isSelectResult.getSqlError().setTable(sel.getOrdNum());
            isSelectResult.getSqlError().setRow(value.getRow());

            return isSelectResult.getSqlError();
        }

        return null;
    }

    private SqlError testPlsqlBlocksPart(SqlRowSet value, TableNode plsql) {

        TableValue<String> entityId = TableValue.createString(value, plsql.getPrimKeyColName());
        TableValue<String> entityBlock = TableValue.createString(value, plsql.getSqlColName());

        if (isSelectStatement(entityBlock.getValue())) {
            // In some cases, the table column can contain PLSQL blocks and SELECT statements
            return null;
        }

        String plsqlBlock = wrapBeginEndIfNeed(entityBlock.getValue());
        plsqlBlock = replaceBindVarsWithAssignmentOperator(plsqlBlock, plsql.getTableName());
        plsqlBlock = removeRowWithValueBindVarIfNeed(plsqlBlock);
        try {
            plsqlBlock = replaceBindVars(plsqlBlock, plsql.getTableName(), plsql.getSqlColName(),
                    entityId.getValue());
        } catch (Exception e) {
            SqlError err = new SqlError("PLSQL-REPLACE-BIND");
            err.setTableName(plsql.getTableName());
            err.setEntityIdColName(plsql.getPrimKeyColName());
            err.setSqlColName(plsql.getSqlColName());
            err.setEntityId(entityId.getValue());
            err.setQuery(plsqlBlock);
            err.setOriginalQuery(entityBlock.getValue());
            err.setTable(plsql.getOrdNum());
            err.setRow(value.getRow());
            err.setErrMsg(e.getMessage());

            return err;
        }

        TableValue<Boolean> plsqlBlockResult = isPlsqlBlock(config, plsqlBlock, plsql.getTableName());
        dropProc = true;
        if (plsqlBlockResult.hasError()) {
            plsqlBlockResult.getSqlError().setTableName(plsql.getTableName());
            plsqlBlockResult.getSqlError().setEntityIdColName(plsql.getPrimKeyColName());
            plsqlBlockResult.getSqlError().setSqlColName(plsql.getSqlColName());
            plsqlBlockResult.getSqlError().setEntityId(entityId.getValue());
            plsqlBlockResult.getSqlError().setQuery(plsqlBlock);
            plsqlBlockResult.getSqlError().setOriginalQuery(entityBlock.getValue());
            plsqlBlockResult.getSqlError().setTable(plsql.getOrdNum());
            plsqlBlockResult.getSqlError().setRow(value.getRow());

            return plsqlBlockResult.getSqlError();
        }

        return null;
    }

    private void testSelectAndPlsqlBlockForAllTables() throws Exception {
        dropProc = false;
        dropView = false;

        for (TableNode sql : selectQuery.values()) {
            testSelectAndPlsqlBlockForAllRows(sql);
        }

        if (dropView) {
            dropViewOrProc(DROP_SELECT_VIEW, "Error when view is deleting {}");
        }

        if (dropProc) {
            dropViewOrProc(DROP_PLSQL_PROC, "Deleting procedure error [{}]");
        }
    }

    private void testSelectAndPlsqlBlockForAllRows(TableNode sql) throws Exception {
        TableValue<SqlRowSet> entitySqls = getSqlRowSetData(sql);
        Long rowCount = 0L;
        if (entitySqls.hasError()) {
            logSqlError(entitySqls.getSqlError());
        } else {
            while (entitySqls.getValue().next()) {
                rowCount++;
                if (testSqlString(entitySqls.getValue(), sql)) {

                    SqlError sqlSelectErr = null;
                    SqlError plSqlBlockErr = null;

                    switch (recognizeSelectOrPlSql(entitySqls.getValue(), sql)) {
                        case SELECT:
                            sqlSelectErr = testSelectStatementPart(entitySqls.getValue(), sql);
                            break;
                        case PL_SQL:
                            plSqlBlockErr = testPlsqlBlocksPart(entitySqls.getValue(), sql);
                            break;
                        case EMPTY:
                            sqlSelectErr = testSelectStatementPart(entitySqls.getValue(), sql);
                            plSqlBlockErr = testPlsqlBlocksPart(entitySqls.getValue(), sql);
                            break;
                    }

                    if (sqlSelectErr != null && plSqlBlockErr != null) {
                        sqlSelectErr.union(plSqlBlockErr);
                        logSqlError(sqlSelectErr);
                    } else if (sqlSelectErr != null) {
                        logSqlError(sqlSelectErr);
                    } else if (plSqlBlockErr != null) {
                        logSqlError(plSqlBlockErr);
                    }
                }
            }
        }

        tableStats.put(sql.getTableName(), rowCount);
    }

    private boolean testSqlString(SqlRowSet value, TableNode sql) {

        TableValue<String> entityId = TableValue.createString(value, sql.getPrimKeyColName());
        if (entityId == null || entityId.hasError()) {
            entityId.getSqlError().setTableName(sql.getTableName());
            entityId.getSqlError().setEntityIdColName(sql.getPrimKeyColName());
            entityId.getSqlError().setSqlColName(sql.getSqlColName());
            entityId.getSqlError().setEntityId(null);
            entityId.getSqlError().setTable(sql.getOrdNum());
            entityId.getSqlError().setRow(value.getRow());

            return false;
        }

        TableValue<String> entitySqlBlock = TableValue.createString(value, sql.getSqlColName());
        if (entitySqlBlock == null || entitySqlBlock.hasError()) {
            entitySqlBlock.getSqlError().setTableName(sql.getTableName());
            entitySqlBlock.getSqlError().setEntityIdColName(sql.getPrimKeyColName());
            entitySqlBlock.getSqlError().setSqlColName(sql.getSqlColName());
            entitySqlBlock.getSqlError().setEntityId(entityId.getValue());
            entitySqlBlock.getSqlError().setTable(sql.getOrdNum());
            entitySqlBlock.getSqlError().setRow(value.getRow());

            logSqlError(entitySqlBlock.getSqlError());
            return false;
        }

        if (StringUtils.isBlank(entitySqlBlock.getValue())) {
            return false;
        }

        return true;
    }

    private void dropViewOrProc(String statement, String errMsg) {
        if (config.isUseSecondTest()) {
            try {
                test2JdbcTemplate.update(statement);
            } catch (DataAccessException e) {
                logger.info(INFO_MARKER, errMsg, e.getMessage());
            }
        } else {
            try {
                test1JdbcTemplate.update(statement);
            } catch (DataAccessException e) {
                logger.info(INFO_MARKER, errMsg, e.getMessage());
            }
        }
    }

    private void logShortError(String table, String column, String pkColumn, String pk, String error) {
        if (StringUtils.isNotBlank(table) && StringUtils.isNotBlank(table) && StringUtils.isNotBlank(table)
                && StringUtils.isNotBlank(table)) {
            logger.info(INFO_MARKER, ERROR_MSG, table, column, pkColumn, pk, error.trim());
        } else {
            logger.info(INFO_MARKER, error.trim());
        }
    }

    private SqlStatementType recognizeSelectOrPlSql(SqlRowSet value, TableNode sel) {

        TableValue<String> entitySql = TableValue.createString(value, sel.getSqlColName());
        String selectSql = new String(entitySql.getValue());

        return SqlParser.recognizeSelectStatementAndPlSql(selectSql);
    }

    private String parseUrlToSchemaWithUrlBeforeDot(String schemaName, String url) {
        url = url.replaceAll(JDBC_THIN_URL_PREFIX, "");
        int colonIndex = url.indexOf(':');
        if (colonIndex != -1) {
            url = url.substring(0, colonIndex);
        } else {
            int slashIndex = url.indexOf('/');
            url = url.substring(0, slashIndex);
        }

        int dotIndex = url.indexOf('.');
        if (dotIndex != -1) {
            url = url.substring(0, dotIndex);
        }

        return MessageFormat.format("{0}@{1}", schemaName, url);
    }

}
