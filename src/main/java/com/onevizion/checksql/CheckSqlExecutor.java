package com.onevizion.checksql;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import javax.annotation.Resource;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import com.onevizion.checksql.exception.UnexpectedException;
import com.onevizion.checksql.vo.AppSettings;
import com.onevizion.checksql.vo.Configuration;
import com.onevizion.checksql.vo.PlsqlBlock;
import com.onevizion.checksql.vo.SelectQuery;
import com.onevizion.checksql.vo.SqlError;

import gudusoft.gsqlparser.TGSqlParser;
import oracle.ucp.jdbc.PoolDataSourceImpl;

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

    private static final String FIND_PLSQL_ERRORS = "select text from all_errors where name = ? and type = 'PROCEDURE'";

    private static final String PLSQL_PROC_NAME = "TEST_PLSQL_BLOCK";

    private static final String DROP_PLSQL_PROC = "drop procedure " + PLSQL_PROC_NAME;

    private static final String VALUE_BIND_VAR = ":VALUE";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public static final Marker INFO_MARKER = MarkerFactory.getMarker("INFO_SQL");

    private static final Marker DATA_MARKER = MarkerFactory.getMarker("DATA_SQL");

    private static final List<String> TABLE_NAMES = Collections.unmodifiableList(Arrays.asList(
            "config_field",
            "excel_orch_mapping",
            "imp_entity_req_field",
            "notif",
            "report_lookup",
            "report_sql",
            "tm_setup",
            "xitor_req_field",
            "imp_data_map",
            "imp_entity",
            "rule_class_param_value",
            "imp_spec",
            "rule",
            "wf_step",
            "wf_template_step"));

    private static final String FIND_FIRST_PROGRAM_ID_NEW = "select program_id from program where rownum < 2 and program_id <> 0";

    private static final String FIND_FIRST_PROGRAM_ID_OLD = "select program_id from v_program where rownum < 2";

    private SqlError sqlError;

    private List<SqlError> sqlErrors;

    public CheckSqlExecutor() {
        super();

        sqlErrors = new ArrayList<SqlError>();
    }

    public void run(Configuration config) {
        logger.info(INFO_MARKER, "SQL Checker is started");

        configAppSettings(config);

        if (config.isEnabledSql()) {
            executeQueries(config);
        } else {
            logger.info(INFO_MARKER, "Testing of SELECT queries is disabled");
        }
        if (config.isEnabledPlSql()) {
            testPlsql(config);
        } else {
            logger.info(INFO_MARKER, "Testing of PLSQL blocks is disabled");
        }
        logSqlErrors();
        logger.info(INFO_MARKER, "SQL Checker is completed");
    }

    private void configAppSettings(Configuration config) {
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

    private void logSqlErrors() {
        if (sqlErrors.isEmpty()) {
            return;
        }

        logTableStats();

        // for (SqlError err : sqlErrors) {
        // logger.warn(err.toString());
        // }
    }

    private void logTableStats() {
        SortedMap<String, Integer> tableStats = new TreeMap<String, Integer>(new Comparator<String>() {

            @Override
            public int compare(String arg0, String arg1) {
                return arg0.compareToIgnoreCase(arg1);
            }

        });

        for (String tableName : TABLE_NAMES) {
            tableStats.put(tableName, 0);
        }

        for (SqlError err : sqlErrors) {
            if (StringUtils.isBlank(err.getTableName())) {
                continue;
            }
            String tableName = new String(err.getTableName()).toLowerCase();
            Integer tableCnt = tableStats.get(tableName);
            tableCnt++;
            tableStats.put(tableName, tableCnt);
        }
        logger.info(INFO_MARKER, "========TABLE STATS=========");
        for (String tableName : tableStats.keySet()) {
            Integer cnt = tableStats.get(tableName);
            logger.info(INFO_MARKER, "table=" + tableName + ", err-count=" + cnt);
        }
    }

    private void executeQueries(Configuration config) {
        int tableNums = SelectQuery.values().length;
        for (SelectQuery sel : SelectQuery.values()) {
            if (!sel.isCheckQuery()) {
                logger.info(INFO_MARKER, "Phase 1/2 Table {}/{} - Check is disabled", sel.getOrdNum(), tableNums);
                continue;
            }
            if (config.getSkipTablesSql().contains(sel.getTableName())) {
                logger.info(INFO_MARKER, "Phase 1/2 Table {}/{} is skipped", sel.getOrdNum(), tableNums);
                continue;
            }
            sqlError = null;

            String sql = sel.getSql();
            String tableName = SqlParser.getFirstTableName(sql);

            SqlRowSet sqlRowSet = getSqlRowSetData(sql);
            if (sqlRowSet == null) {
                sqlError.setTableName(tableName);
                sqlErrors.add(sqlError);
                logger.info(INFO_MARKER, "Phase 1/2 Table {}/{} - Getting data error [{}]", sel.getOrdNum(), tableNums,
                        sqlError.toString());
                continue;
            }

            List<String> sqlDataCols = SqlParser.getCols(sql);
            String entityIdColName = sqlDataCols.get(0);
            String sqlColName = sqlDataCols.get(1);

            boolean isEmptyTable = true;
            while (sqlRowSet.next()) {
                isEmptyTable = false;
                sqlError = null;
                logger.info(INFO_MARKER, "Phase 1/2 Table {}/{} Row {}/{}", sel.getOrdNum(), tableNums,
                        sqlRowSet.getRow(), sqlRowSet.getString(SelectQuery.TOTAL_ROWS_COL_NAME));

                String entityId = sqlRowSet.getString(1);

                String entitySql = getStringVal(sqlRowSet, 2);
                if (sqlError != null) {
                    sqlError.setTableName(tableName);
                    sqlError.setEntityIdColName(entityIdColName);
                    sqlError.setSqlColName(sqlColName);
                    sqlError.setEntityId(entityId);
                    logSqlError(sqlError);
                    continue;
                }

                String replacedImpVars = new String(entitySql);
                if ("imp_data_type_param".equalsIgnoreCase(tableName)) {
                    replacedImpVars = replaceStaticImpDataTypeParam(replacedImpVars);
                }

                TGSqlParser parser = parseSelectQuery(replacedImpVars);
                if (sqlError != null) {
                    sqlError.setTableName(tableName);
                    sqlError.setEntityIdColName(entityIdColName);
                    sqlError.setSqlColName(sqlColName);
                    sqlError.setEntityId(entityId);
                    sqlError.setQuery(replacedImpVars);
                    sqlError.setOriginalQuery(entitySql);
                    logSqlError(sqlError);
                    continue;
                }
                if (SqlParser.isSelectStatement(parser)) {
                    String sqlWoutPlaceholder = new String(replacedImpVars);
                    if (sqlWoutPlaceholder.contains("?")) {
                        sqlWoutPlaceholder = sqlWoutPlaceholder.replace("?", ":p");
                    }

                    String preparedSql = SqlParser.removeIntoClause(sqlWoutPlaceholder);
                    preparedSql = removeSemicolonAtTheEnd(preparedSql);
                    testSelectQuery(preparedSql, config);
                    if (sqlError != null) {
                        sqlError.setTableName(tableName);
                        sqlError.setEntityIdColName(entityIdColName);
                        sqlError.setSqlColName(sqlColName);
                        sqlError.setEntityId(entityId);
                        sqlError.setQuery(preparedSql);
                        sqlError.setOriginalQuery(entitySql);
                        logSqlError(sqlError);
                        continue;
                    }
                }
            }
            if (isEmptyTable) {
                logger.info(INFO_MARKER, "Phase 1/2 Table {}/{} Row 0/0", sel.getOrdNum(), tableNums);
            }
        }
    }

    private void logSqlError(SqlError sqlError) {
        logger.info(DATA_MARKER, "{}", sqlError.toString());
        sqlErrors.add(sqlError);
    }

    private void testPlsql(Configuration config) {
        boolean isProc1Created = false;
        boolean isProc2Created = false;
        int tableNums = PlsqlBlock.values().length;
        for (PlsqlBlock plsql : PlsqlBlock.values()) {
            if (!plsql.isCheckQuery()) {
                logger.info(INFO_MARKER, "Phase 2/2 Table {}/{} - Check is disabled", plsql.getOrdNum(), tableNums);
                continue;
            }
            if (config.getSkipTablesPlSql().contains(plsql.getTableName())) {
                logger.info(INFO_MARKER, "Phase 2/2 Table {}/{} is skipped", plsql.getOrdNum(), tableNums);
                continue;
            }
            sqlError = null;

            String sql = plsql.getSql();
            String tableName = SqlParser.getFirstTableName(sql);
            List<String> sqlDataCols = SqlParser.getCols(sql);
            String entityIdColName = sqlDataCols.get(0);

            String sqlColName = sqlDataCols.get(1);

            SqlRowSet sqlRowSet = getSqlRowSetData(sql);
            if (sqlRowSet == null) {
                sqlError.setTableName(tableName);
                sqlErrors.add(sqlError);
                logger.info(INFO_MARKER, "Phase 2/2 Table {}/{} - Getting data error [{}]", plsql.getOrdNum(),
                        tableNums, sqlError.toString());
                continue;
            }

            boolean isEmptyTable = true;
            while (sqlRowSet.next()) {
                isEmptyTable = false;
                sqlError = null;

                logger.info(INFO_MARKER, "Phase 2/2 Table {}/{} Row {}/{}", plsql.getOrdNum(), tableNums,
                        sqlRowSet.getRow(), sqlRowSet.getString(SelectQuery.TOTAL_ROWS_COL_NAME));

                String entityId = sqlRowSet.getString(1);

                String entityBlock = getStringVal(sqlRowSet, 2);
                if (sqlError != null) {
                    sqlError.setTableName(tableName);
                    sqlError.setEntityIdColName(entityIdColName);
                    sqlError.setSqlColName(sqlColName);
                    sqlError.setEntityId(entityId);
                    logSqlError(sqlError);
                    continue;
                }

                String beginEndStatement = wrapBeginEndIfNeed(entityBlock);
                boolean isSelectStatement;
                try {
                    isSelectStatement = SqlParser.isSelectStatement(beginEndStatement);
                } catch (Exception e1) {
                    sqlError = new SqlError("PARSE-BLOCK");
                    sqlError.setTableName(tableName);
                    sqlError.setEntityIdColName(entityIdColName);
                    sqlError.setSqlColName(sqlColName);
                    sqlError.setEntityId(entityId);
                    sqlError.setErrMsg(e1.getMessage());
                    sqlError.setQuery(beginEndStatement);
                    sqlError.setOriginalQuery(entityBlock);
                    logSqlError(sqlError);
                    continue;
                }

                if (isSelectStatement) {
                    continue;
                }

                String woutBindVarsBlock = replaceBindVars(beginEndStatement, tableName, sqlColName, entityId);
                woutBindVarsBlock = removeRowWithValueBindVarIfNeed(woutBindVarsBlock);
                String wrappedBlockAsProc = wrapBlockAsProc(woutBindVarsBlock);
                if (config.isUseSecondTest()) {
                    try {
                        isProc2Created = false;
                        test2JdbcTemplate.update(wrappedBlockAsProc);
                        isProc2Created = true;
                    } catch (DataAccessException e2) {
                        sqlError = new SqlError("CREATE-PROC2");
                        sqlError.setTableName(tableName);
                        sqlError.setEntityIdColName(entityIdColName);
                        sqlError.setSqlColName(sqlColName);
                        sqlError.setEntityId(entityId);
                        sqlError.setErrMsg(e2.getMessage());
                        sqlError.setQuery(wrappedBlockAsProc);
                        sqlError.setOriginalQuery(entityBlock);
                        logSqlError(sqlError);
                    }
                } else {
                    try {
                        isProc1Created = false;
                        test1JdbcTemplate.update(wrappedBlockAsProc);
                        isProc1Created = true;
                    } catch (DataAccessException e) {
                        sqlError = new SqlError("CREATE-PROC1");
                        sqlError.setTableName(tableName);
                        sqlError.setEntityIdColName(entityIdColName);
                        sqlError.setSqlColName(sqlColName);
                        sqlError.setEntityId(entityId);
                        sqlError.setErrMsg(e.getMessage());
                        sqlError.setQuery(wrappedBlockAsProc);
                        sqlError.setOriginalQuery(entityBlock);
                        logSqlError(sqlError);
                    }
                }

                if (!isProc1Created && !isProc2Created) {
                    continue;
                }

                if (isProc2Created) {
                    SqlRowSet procErrSqlRowSet = test2JdbcTemplate.queryForRowSet(FIND_PLSQL_ERRORS,
                            PLSQL_PROC_NAME);
                    if (procErrSqlRowSet.next()) {
                        String errMsg = getStringVal(procErrSqlRowSet, 1);
                        if (StringUtils.isNotBlank(errMsg)) {
                            sqlError = new SqlError("PLSQL2");
                            sqlError.setTableName(tableName);
                            sqlError.setEntityIdColName(entityIdColName);
                            sqlError.setSqlColName(sqlColName);
                            sqlError.setEntityId(entityId);
                            sqlError.setErrMsg(errMsg);
                            sqlError.setQuery(wrappedBlockAsProc);
                            sqlError.setOriginalQuery(entityBlock);
                            logSqlError(sqlError);
                            continue;
                        }
                    }
                } else if (isProc1Created) {
                    SqlRowSet procErrSqlRowSet = test1JdbcTemplate.queryForRowSet(FIND_PLSQL_ERRORS,
                            PLSQL_PROC_NAME);
                    if (procErrSqlRowSet.next()) {
                        String errMsg = getStringVal(procErrSqlRowSet, 1);
                        if (StringUtils.isNotBlank(errMsg)) {
                            sqlError = new SqlError("PLSQL1");
                            sqlError.setTableName(tableName);
                            sqlError.setEntityIdColName(entityIdColName);
                            sqlError.setSqlColName(sqlColName);
                            sqlError.setEntityId(entityId);
                            sqlError.setErrMsg(errMsg);
                            sqlError.setQuery(wrappedBlockAsProc);
                            sqlError.setOriginalQuery(entityBlock);
                            logSqlError(sqlError);
                            continue;
                        }
                    }
                } else {
                    continue;
                }
            }
            if (isEmptyTable) {
                logger.info(INFO_MARKER, "Phase 2/2 Table {}/{} Row 0/0", plsql.getOrdNum(), tableNums);
            }
        }

        if (isProc1Created) {
            try {
                test1JdbcTemplate.update(DROP_PLSQL_PROC);
            } catch (DataAccessException e) {
                logger.info(INFO_MARKER, "Phase 2/2 Test 1 Deleting procedure error [{}]", e.getMessage());
            }

        }

        if (isProc2Created) {
            try {
                test2JdbcTemplate.update(DROP_PLSQL_PROC);
            } catch (DataAccessException e) {
                logger.info(INFO_MARKER, "Phase 2/2 Test 2 Deleting procedure error [{}]", e.getMessage());
            }
        }
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
            String newSql = replaceImpDataTypeParamByImpDataTypeId(sql, entityId);
            newSql = replaceStaticImpDataTypeParam(newSql);
            return newSql;
        } else if ("imp_entity".equalsIgnoreCase(tableName)) {
            String newSql = replaceImpEntityParamsByEntityId(sql, entityId);
            return newSql;
        } else if ("imp_spec".equalsIgnoreCase(tableName)) {
            String newSql = replaceImpSpecExtProcParams(sql);
            return newSql;
        } else if ("rule".equalsIgnoreCase(tableName) && "sql_text".equalsIgnoreCase(sqlColName)) {
            String newSql = replaceRuleParams(sql, entityId);
            return newSql;
        } else if ("wf_template_step".equalsIgnoreCase(tableName) || "wf_step".equalsIgnoreCase(tableName)) {
            String newSql = replaceWfStepParams(sql);
            return newSql;
        } else {
            return sql;
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
                newSql = newSql.replaceAll(":" + sqlParam, "0");
            }
        }

        newSql = newSql.replaceAll(":ENTITY_PK", "0");
        newSql = newSql.replaceAll(":VALUE", "0");
        return newSql;
    }

    private String wrapBeginEndIfNeed(String entityBlock) {
        String str = new String(entityBlock);
        str = str.trim().toLowerCase();
        if (str.startsWith("select") || str.startsWith("declare") || str.startsWith("begin")) {
            return entityBlock;
        } else {
            return "begin\r\n" + entityBlock + "\r\nend;";
        }
    }

    private String wrapBlockAsProc(String entityBlock) {
        StringBuilder ddl = new StringBuilder("create or replace procedure ");
        ddl.append(PLSQL_PROC_NAME);
        ddl.append(" as\r\n v_ret_str varchar2(1000);\r\nbegin\r\n");
        ddl.append(entityBlock);
        ddl.append("\r\nend ");
        ddl.append(PLSQL_PROC_NAME);
        ddl.append(";");
        return ddl.toString();
    }

    private boolean testSelectQuery(String sql, Configuration configuration) {
        String limitedSql = "select * from (\r\n" + sql + "\r\n) where rownum = 1";
        TGSqlParser pareparedSqlParser = SqlParser.getParser(limitedSql);
        Map<String, Object> paramMap = getSqlParamMap(pareparedSqlParser);
        if (configuration.isUseSecondTest()) {
            try {
                test2NamedParamJdbcTemplate.queryForRowSet(limitedSql, paramMap);
            } catch (DataAccessException e) {
                // try {
                // test1NamedParamJdbcTemplate.queryForRowSet(sql,
                // paramMap);
                // } catch (DataAccessException e2) {
                sqlError = new SqlError(SqlError.SELECT_ERR_TYPE + "2");
                sqlError.setErrMsg(e.getMessage());
                return false;
                // }
            }
        } else {
            try {
                test1NamedParamJdbcTemplate.queryForRowSet(limitedSql, paramMap);
            } catch (DataAccessException e) {
                sqlError = new SqlError(SqlError.SELECT_ERR_TYPE + "1");
                sqlError.setErrMsg(e.getMessage());
                return false;
            }
        }
        return true;
    }

    private String removeSemicolonAtTheEnd(String sql) {
        String newSql = new String(sql);
        if (newSql.endsWith(";")) {
            newSql = newSql.substring(0, newSql.length() - 1);
        }
        return newSql;
    }

    private String replaceStaticImpDataTypeParam(String sql) {
        String newSql = new String(sql);
        newSql = newSql.replaceAll("\\[USER_ID\\]", "p");
        newSql = newSql.replaceAll("\\[PROGRAM_ID\\]", "p");
        newSql = newSql.replaceAll("\\[DATE_FORMAT\\]", "p");
        newSql = newSql.replaceAll("\\[COLUMN_NAME\\]", "p");
        return newSql;
    }

    private Map<String, Object> getSqlParamMap(TGSqlParser parser) {
        List<String> sqlParams = SqlParser.getParams(parser);
        Map<String, Object> paramMap = new HashMap<String, Object>();
        for (String paramName : sqlParams) {
            paramMap.put(paramName, "0");
        }
        return paramMap;
    }

    private TGSqlParser parseSelectQuery(String selectQuery) {
        TGSqlParser parser;
        try {
            parser = SqlParser.getParser(selectQuery);
        } catch (Exception e) {
            sqlError = new SqlError("PARSE-QUERY");
            sqlError.setErrMsg(e.getMessage());
            parser = null;
        }
        return parser;
    }

    private String getStringVal(SqlRowSet sqlRowSet, int numCol) {
        int colType = sqlRowSet.getMetaData().getColumnType(numCol);
        String entitySql;
        if (Types.CLOB == colType) {
            Clob clobObj = (Clob) sqlRowSet.getObject(numCol);

            InputStream in;
            try {
                in = clobObj.getAsciiStream();
            } catch (SQLException e3) {
                sqlError = new SqlError("Clob2Stream");
                sqlError.setErrMsg(e3.getMessage());
                return null;
            }
            StringWriter w = new StringWriter();
            try {
                IOUtils.copy(in, w);
            } catch (IOException e3) {
                sqlError = new SqlError("Stream2Writer");
                sqlError.setErrMsg(e3.getMessage());
                return null;
            }
            entitySql = w.toString();
        } else {
            entitySql = sqlRowSet.getString(numCol);
        }
        if (StringUtils.isBlank(entitySql)) {
            sqlError = new SqlError("BLANK-SQL");
            sqlError.setErrMsg("SQL is blank");
            return null;
        }
        return entitySql;
    }

    private SqlRowSet getSqlRowSetData(String sql) {
        SqlRowSet sqlRowSet;
        try {
            sqlRowSet = owner1JdbcTemplate.queryForRowSet(sql);
        } catch (DataAccessException e1) {
            sqlError = new SqlError("SELECT-ENTITY");
            sqlError.setErrMsg(e1.getMessage());
            sqlRowSet = null;
        }
        return sqlRowSet;
    }
}
