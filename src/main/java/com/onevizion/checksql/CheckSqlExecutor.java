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
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import gudusoft.gsqlparser.TGSqlParser;

@Component
public class CheckSqlExecutor {

    @Resource(name = "ownerJdbcTemplate")
    private JdbcTemplate ownerJdbcTemplate;

    @Resource(name = "testJdbcTemplate")
    private JdbcTemplate testJdbcTemplate;

    @Resource(name = "testNamedParamJdbcTemplate")
    private NamedParameterJdbcTemplate testNamedParamJdbcTemplate;

    private static final String SET_PID = "call pkg_sec.set_pid(?)";

    private static final String FIND_FIRST_PROGRAM_ID = "select program_id from program where rownum < 2";

    private static final String FIND_IMP_DATA_TYPE_PARAM_SQL_PARAM_BY_IMP_DATA_TYPE_ID = "select sql_parameter from imp_data_type_param where imp_data_type_id = ?";

    private static final String FIND_IMP_ENTITY_PARAM_SQL_PARAM_BY_ENTITY_ID = "select sql_parameter from imp_entity_param where imp_entity_id = ?";

    private static final String FIND_RULE_PARAM_SQL_PARAM_BY_ENTITY_ID = "select ID_FIELD from rule r join rule_type t on (r.rule_type_id = t.rule_type_id) where r.rule_id = ?";

    private static final String FIND_PLSQL_ERRORS = "select text from all_errors where name = ? and type = 'PROCEDURE'";

    private static final String PLSQL_PROC_NAME = "TEST_PLSQL_BLOCK";

    private static final String DROP_PLSQL_PROC = "drop procedure " + PLSQL_PROC_NAME;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    // Query format: select [program_id], <entity_id>, <SQL> from <table>...
    private static final List<String> SELECT_QUERIES = Collections.unmodifiableList(Arrays.asList(
            "select program_id, config_field_id, DEFAULT_VALUE_SQL from config_field where DEFAULT_VALUE_SQL is not null",
            "select program_id, config_field_id, SQL_QUERY from config_field where SQL_QUERY is not null",
            "select program_id, EXCEL_ORCH_MAPPING_ID, DEFAULT_VALUE_SQL from excel_orch_mapping where DEFAULT_VALUE_SQL is not null",
            "select program_id, EXCEL_ORCH_MAPPING_ID, SQL_QUERY from excel_orch_mapping where SQL_QUERY is not null",
            "select grid_page_field_id, cell_renderer_param1 from grid_page_field where cell_renderer_id = 78",
            "select program_id, IMP_ENTITY_REQ_FIELD_ID, sql_text from imp_entity_req_field where sql_text is not null",
            "select program_id, notif_id, trackor_sql from notif where trackor_sql is not null",
            "select program_id, notif_id, user_sql from notif where user_sql is not null",
            "select program_id, param_program_id, param_sql from param_program where param_sql is not null",
            "select param_system_id, param_sql from param_system where param_sql is not null",
            "select program_id, report_lookup_id, lookup_sql from report_lookup",
            "select program_id, report_run_sql_id, sql_text from report_run_sql",
            "select program_id, report_sql_id, sql_text from report_sql where sql_text is not null",
            "select rule_class_param_id, sql_text from rule_class_param where sql_text is not null",
            "select rule_type_id, template_sql from rule_type where template_sql is not null",
            "select program_id, tm_setup_id, search_sql from tm_setup where search_sql is not null and length(search_sql) > 0",
            "select program_id, XITOR_REQ_FIELD_ID, DEFAULT_VALUE_SQL from xitor_req_field where DEFAULT_VALUE_SQL is not null",
            "select imp_data_type_param_id, sql_text from imp_data_type_param where sql_text is not null",
            "select program_id, imp_data_map_id, sql_text from imp_data_map where sql_text is not null",
            "select program_id, imp_entity_id, sql_text from imp_entity where sql_text is not null",
            "select program_id, rule_class_param_value_id, value_clob from rule_class_param_value where value_clob is not null"));

    private static final List<String> PLSQL_BLOCKS = Collections.unmodifiableList(Arrays.asList(
            "select program_id, imp_data_map_id, sql_text from imp_data_map where sql_text is not null",
            "select imp_data_type_id, sql_text from imp_data_type where sql_text is not null",
            "select program_id, imp_entity_id, sql_text from imp_entity where sql_text is not null",
            "select program_id, imp_spec_id, external_proc from imp_spec where external_proc is not null",
            "select program_id, rule_id, sql_text from rule where sql_text is not null",
            "select program_id, rule_class_param_value_id, value_clob from rule_class_param_value where value_clob is not null",
            "select program_id, wf_step_id, plsql_block from wf_step where plsql_block is not null",
            "select program_id, wf_template_step_id, plsql_block from wf_template_step where plsql_block is not null"));

    private SqlError sqlError;

    private List<SqlError> sqlErrors = new ArrayList<SqlError>();

    public void run() {
        logger.info("SQL Checker is started");
        executeQueries(SELECT_QUERIES);
        testPlsql(PLSQL_BLOCKS);
        logSqlErrors();
        logger.info("SQL Checker is completed");
    }

    private void logSqlErrors() {
        if (sqlErrors.isEmpty()) {
            return;
        }
        SortedMap<String, Integer> tableStats = new TreeMap<String, Integer>(new Comparator<String>() {

            @Override
            public int compare(String arg0, String arg1) {
                return arg0.compareToIgnoreCase(arg1);
            }

        });
        for (SqlError err : sqlErrors) {
            if (StringUtils.isBlank(err.getTableName())) {
                continue;
            }
            String tableName = new String(err.getTableName()).toLowerCase();
            Integer tableCnt;
            if (tableStats.containsKey(tableName)) {
                tableCnt = tableStats.get(tableName);
            } else {
                tableCnt = 0;
            }
            tableCnt++;
            tableStats.put(tableName, tableCnt);
        }
        logger.info("========TABLE STATS=========");
        for (String tableName : tableStats.keySet()) {
            Integer cnt = tableStats.get(tableName);
            logger.info("table=" + tableName + ", err-count=" + cnt);
        }

        for (SqlError err : sqlErrors) {
            logger.warn(err.toString());
        }
    }

    private void executeQueries(List<String> queries) {
        for (String sql : queries) {
            sqlError = null;

            String tableName = SqlParser.getFirstTableName(sql);
            List<String> sqlDataCols = SqlParser.getCols(sql);
            boolean programSpeficic = sqlDataCols.size() == 3;
            String entityIdColName = programSpeficic ? sqlDataCols.get(1) : sqlDataCols.get(0);

            String sqlColName;
            if (programSpeficic) {
                sqlColName = sqlDataCols.get(2);
            } else {
                sqlColName = sqlDataCols.get(1);
            }

            SqlRowSet sqlRowSet = getSqlRowSetData(sql);
            if (sqlRowSet == null) {
                sqlError.setTableName(tableName);
                sqlErrors.add(sqlError);
                continue;
            }

            if (!programSpeficic) {
                setRandomProgramId();
                if (sqlError != null) {
                    sqlError.setTableName(tableName);
                    sqlError.setEntityIdColName(entityIdColName);
                    sqlError.setSqlColName(sqlColName);
                    sqlErrors.add(sqlError);
                    continue;
                }
            }

            while (sqlRowSet.next()) {
                sqlError = null;

                String entityId;
                if (programSpeficic) {
                    entityId = sqlRowSet.getString(2);
                } else {
                    entityId = sqlRowSet.getString(1);
                }

                String entitySql = getStringVal(sqlRowSet, (programSpeficic ? 3 : 2));
                if (sqlError != null) {
                    sqlError.setTableName(tableName);
                    sqlError.setEntityIdColName(entityIdColName);
                    sqlError.setSqlColName(sqlColName);
                    sqlError.setEntityId(entityId);
                    sqlErrors.add(sqlError);
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
                    sqlErrors.add(sqlError);
                    continue;
                }
                if (SqlParser.isSelectStatement(parser)) {
                    String sqlWoutPlaceholder = new String(replacedImpVars);
                    if (sqlWoutPlaceholder.contains("?")) {
                        sqlWoutPlaceholder = sqlWoutPlaceholder.replace("?", ":p");
                    }
                    if (sqlDataCols.size() == 3) {
                        setProgramId(sqlRowSet);
                        if (sqlError != null) {
                            sqlError.setTableName(tableName);
                            sqlError.setEntityIdColName(entityIdColName);
                            sqlError.setSqlColName(sqlColName);
                            sqlError.setEntityId(entityId);
                            sqlErrors.add(sqlError);
                            continue;
                        }
                    }

                    String preparedSql = SqlParser.removeIntoClause(sqlWoutPlaceholder);
                    preparedSql = removeSemicolonAtTheEnd(preparedSql);
                    testSelectQuery(preparedSql);
                    if (sqlError != null) {
                        sqlError.setTableName(tableName);
                        sqlError.setEntityIdColName(entityIdColName);
                        sqlError.setSqlColName(sqlColName);
                        sqlError.setEntityId(entityId);
                        sqlError.setQuery(preparedSql);
                        sqlError.setOriginalQuery(entitySql);
                        sqlErrors.add(sqlError);
                        continue;
                    }
                }
            }
        }
    }

    private void testPlsql(List<String> queries) {
        boolean isProcCreated = false;
        for (String sql : queries) {
            sqlError = null;

            String tableName = SqlParser.getFirstTableName(sql);
            List<String> sqlDataCols = SqlParser.getCols(sql);
            boolean programSpeficic = sqlDataCols.size() == 3;
            String entityIdColName = programSpeficic ? sqlDataCols.get(1) : sqlDataCols.get(0);

            String sqlColName;
            if (programSpeficic) {
                sqlColName = sqlDataCols.get(2);
            } else {
                sqlColName = sqlDataCols.get(1);
            }

            SqlRowSet sqlRowSet = getSqlRowSetData(sql);
            if (sqlRowSet == null) {
                sqlError.setTableName(tableName);
                sqlErrors.add(sqlError);
                continue;
            }

            while (sqlRowSet.next()) {
                sqlError = null;

                String entityId;
                if (programSpeficic) {
                    entityId = sqlRowSet.getString(2);
                } else {
                    entityId = sqlRowSet.getString(1);
                }

                String entityBlock = getStringVal(sqlRowSet, (programSpeficic ? 3 : 2));
                if (sqlError != null) {
                    sqlError.setTableName(tableName);
                    sqlError.setEntityIdColName(entityIdColName);
                    sqlError.setSqlColName(sqlColName);
                    sqlError.setEntityId(entityId);
                    sqlErrors.add(sqlError);
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
                    sqlErrors.add(sqlError);
                    continue;
                }

                if (isSelectStatement) {
                    continue;
                }

                String woutBindVarsBlock = replaceBindVars(beginEndStatement, tableName, sqlColName, entityId);
                String wrappedBlockAsProc = wrapBlockAsProc(woutBindVarsBlock);
                try {
                    testJdbcTemplate.update(wrappedBlockAsProc);
                } catch (DataAccessException e) {
                    sqlError = new SqlError("CREATE-PROC");
                    sqlError.setTableName(tableName);
                    sqlError.setEntityIdColName(entityIdColName);
                    sqlError.setSqlColName(sqlColName);
                    sqlError.setEntityId(entityId);
                    sqlError.setErrMsg(e.getMessage());
                    sqlError.setQuery(wrappedBlockAsProc);
                    sqlError.setOriginalQuery(entityBlock);
                    sqlErrors.add(sqlError);
                    continue;
                }
                isProcCreated = true;

                SqlRowSet procErrSqlRowSet = testJdbcTemplate.queryForRowSet(FIND_PLSQL_ERRORS, PLSQL_PROC_NAME);
                if (procErrSqlRowSet.next()) {
                    String errMsg = getStringVal(procErrSqlRowSet, 1);
                    if (StringUtils.isNotBlank(errMsg)) {
                        sqlError = new SqlError("PLSQL");
                        sqlError.setTableName(tableName);
                        sqlError.setEntityIdColName(entityIdColName);
                        sqlError.setSqlColName(sqlColName);
                        sqlError.setEntityId(entityId);
                        sqlError.setErrMsg(errMsg);
                        sqlError.setQuery(wrappedBlockAsProc);
                        sqlError.setOriginalQuery(entityBlock);
                        sqlErrors.add(sqlError);
                        continue;
                    }
                }
            }
        }

        if (isProcCreated) {
            try {
                testJdbcTemplate.update(DROP_PLSQL_PROC);
            } catch (DataAccessException e) {
                logger.warn(
                        "[DROP-PROC][" + DROP_PLSQL_PROC + "]: " + e.getMessage() + "\r\n");
            }
        }
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
        List<String> params = ownerJdbcTemplate.queryForList(FIND_RULE_PARAM_SQL_PARAM_BY_ENTITY_ID,
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
        List<String> sqlParams = ownerJdbcTemplate.queryForList(FIND_IMP_ENTITY_PARAM_SQL_PARAM_BY_ENTITY_ID,
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
        List<String> sqlParams = ownerJdbcTemplate.queryForList(FIND_IMP_DATA_TYPE_PARAM_SQL_PARAM_BY_IMP_DATA_TYPE_ID,
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

    private boolean setRandomProgramId() {
        Long pid = ownerJdbcTemplate.queryForObject(FIND_FIRST_PROGRAM_ID, Long.class);
        try {
            testJdbcTemplate.update(SET_PID, pid);
        } catch (DataAccessException e1) {
            sqlError = new SqlError("(RND)PKG_SEC.SET_PID");
            sqlError.setErrMsg(e1.getMessage());
            return false;
        }
        return true;
    }

    private boolean testSelectQuery(String sql) {

        TGSqlParser pareparedSqlParser = SqlParser.getParser(sql);
        Map<String, Object> paramMap = getSqlParamMap(pareparedSqlParser);
        try {
            testNamedParamJdbcTemplate.queryForRowSet(sql, paramMap);
        } catch (DataAccessException e) {
            sqlError = new SqlError(SqlError.SELECT_ERR_TYPE);
            sqlError.setErrMsg(e.getMessage());
            return false;
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

    private boolean setProgramId(SqlRowSet sqlRowSet) {
        Long pid = sqlRowSet.getLong(1);
        try {
            testJdbcTemplate.update(SET_PID, pid);
        } catch (DataAccessException e1) {
            sqlError = new SqlError("PKG_SEC.SET_PID");
            sqlError.setErrMsg(e1.getMessage());
            return false;
        }
        return true;
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
            sqlRowSet = ownerJdbcTemplate.queryForRowSet(sql);
        } catch (DataAccessException e1) {
            sqlError = new SqlError("SELECT-ENTITY");
            sqlError.setErrMsg(e1.getMessage());
            sqlRowSet = null;
        }
        return sqlRowSet;
    }
}
