package com.onevizion.checksql;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.InvalidResultSetAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;

import com.onevizion.checksql.exception.SqlParsingException;
import com.onevizion.checksql.exception.UnexpectedException;
import com.onevizion.checksql.vo.AppSettings;
import com.onevizion.checksql.vo.CheckSqlQuery;
import com.onevizion.checksql.vo.Configuration;
//import com.onevizion.checksql.vo.PlsqlBlock;
import com.onevizion.checksql.vo.SelectQuery;
import com.onevizion.checksql.vo.SqlError;
import com.onevizion.checksql.vo.TableNode;
import com.onevizion.checksql.vo.TableValue;

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

    private static final String FIND_DB_OBJECT_ERRORS = "select text from all_errors where name = ? and type = 'PROCEDURE'";

    private static final String PLSQL_PROC_NAME = "CHECKSQL_PLSQL";

    private static final String SELECT_VIEW_NAME = "CHECKSQL_SELECT";

    private static final String DROP_PLSQL_PROC = "drop procedure " + PLSQL_PROC_NAME;

    private static final String DROP_SELECT_VIEW = "drop view " + SELECT_VIEW_NAME;

    private static final String VALUE_BIND_VAR = ":VALUE";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public static final Marker INFO_MARKER = MarkerFactory.getMarker("INFO_SQL");

    private static final Marker DATA_MARKER = MarkerFactory.getMarker("DATA_SQL");
    
    private static /*final*/ SelectQuery selectQuery;// = new SelectQuery(); 
    
    //private static final PlsqlBlock plsqlBlock = new PlsqlBlock(); 

    private static final String FIND_FIRST_PROGRAM_ID_NEW = "select program_id from program where rownum < 2 and program_id <> 0";

    private static final String FIND_FIRST_PROGRAM_ID_OLD = "select program_id from v_program where rownum < 2";

    private List<SqlError> sqlErrors;

    public CheckSqlExecutor() {
        sqlErrors = new ArrayList<SqlError>();
    }

    public void run(Configuration config) {
        logger.info(INFO_MARKER, "SQL Checker is started");

        configAppSettings(config);
        try {
            selectQuery = new SelectQuery(owner1JdbcTemplate); 
        } catch (Exception e) {
            logger.info(INFO_MARKER, "SQL Checker is failed with error\r\n{}", e);
            return;
        }
        /*if (config.isEnabledSql()) {
            try {
                testSelectQueries(config);
            } catch (Exception e) {
                logger.info(INFO_MARKER, "SQL Checker is failed with error\r\n{}", e);
                return;
            }
        } else {
            logger.info(INFO_MARKER, "Testing of SELECT queries is disabled");
        }*/
        try {
            testSelectQueries(config);
        } catch (Exception e) {
            logger.info(INFO_MARKER, "SQL Checker is failed with error\r\n{}", e);
            return;
        }
        /*if (config.isEnabledPlSql()) {
            try {
                testPlsqlBlocks(config);
            } catch (Exception e) {
                logger.info(INFO_MARKER, "SQL Checker is failed with error\r\n{}", e);
                return;
            }
        } else {
            logger.info(INFO_MARKER, "Testing of PLSQL blocks is disabled");
        }*/
        try {
            testPlsqlBlocks(config);
        } catch (Exception e) {
            logger.info(INFO_MARKER, "SQL Checker is failed with error\r\n{}", e);
            return;
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
    }

    private void logTableStats() {
        SortedMap<String, Integer> tableStats = new TreeMap<String, Integer>(new Comparator<String>() {

            @Override
            public int compare(String arg0, String arg1) {
                return arg0.compareToIgnoreCase(arg1);
            }

        });
  
        for (TableNode select : selectQuery.values()) {
            tableStats.put(select.getTableName().toLowerCase(), 0);
        }

        for (TableNode plsql : /*plsqlBlock*/selectQuery.values()) {
            tableStats.put(plsql.getTableName().toLowerCase(), 0);
        }

        for (SqlError err : sqlErrors) {
            if (StringUtils.isBlank(err.getTableName())) {
                continue;
            }
            String tableName = err.getTableName().toLowerCase();
            Integer count = tableStats.get(tableName);
            count++;
            tableStats.put(tableName, count);
        }
        logger.info(INFO_MARKER, "========TABLE STATS=========");
        for (String tableName : tableStats.keySet()) {
            Integer cnt = tableStats.get(tableName);
            logger.info(INFO_MARKER, "table=" + tableName + ", err-count=" + cnt);
        }
    }

    private void testSelectQueries(Configuration config) throws InvalidResultSetAccessException, Exception {
        final int tableNums = selectQuery.values().size();

        boolean dropView = false;
        for (TableNode sel : selectQuery.values()) {
            /*if (config.isSkippedSqlTable(sel.getTableName())) {
                logger.info(INFO_MARKER, "Phase 1/2 Table {}/{}: Table {} is skipped", sel.getOrdNum(), tableNums,
                        sel.getTableName());
                continue;
            }*/

            TableValue<SqlRowSet> entitySqls = getSqlRowSetData(sel);
            if (entitySqls.hasError()) {
                logger.info(INFO_MARKER,
                        "Phase 1/2 Table {}/{}: Error to get of list with SELECT queries for {}.{}\r\n{}",
                        sel.getOrdNum(), tableNums, sel.getTableName(), sel.getSqlColName(),
                        entitySqls.getSqlError().toString());
                continue;
            }

            boolean isEmptyTable = true;
            while (entitySqls.getValue().next()) {
                isEmptyTable = false;

                TableValue<String> entityId = TableValue.createString(entitySqls.getValue(), sel.getPrimKeyColName());
                if (entityId == null || entityId.hasError()) {
                    entityId.getSqlError().setTableName(sel.getTableName());
                    entityId.getSqlError().setEntityIdColName(sel.getPrimKeyColName());
                    entityId.getSqlError().setSqlColName(sel.getSqlColName());
                    entityId.getSqlError().setEntityId(null);
                    entityId.getSqlError().setPhase(1);
                    entityId.getSqlError().setTable(sel.getOrdNum());
                    entityId.getSqlError().setRow(entitySqls.getValue().getRow());
                    logger.info(INFO_MARKER, "Phase 1/2 Table {}/{} Row {}/{}: Error\r\n{}", sel.getOrdNum(), tableNums,
                            entitySqls.getValue().getRow(),
                            entitySqls.getValue().getString(selectQuery.TOTAL_ROWS_COL_NAME),
                            entityId.getSqlError().getErrMsg());
                    continue;
                }

                TableValue<String> entitySql = TableValue.createString(entitySqls.getValue(), sel.getSqlColName());
                if (entitySql == null || entitySql.hasError()) {
                    entitySql.getSqlError().setTableName(sel.getTableName());
                    entitySql.getSqlError().setEntityIdColName(sel.getPrimKeyColName());
                    entitySql.getSqlError().setSqlColName(sel.getSqlColName());
                    entitySql.getSqlError().setEntityId(entityId.getValue());
                    entitySql.getSqlError().setPhase(1);
                    entitySql.getSqlError().setTable(sel.getOrdNum());
                    entitySql.getSqlError().setRow(entitySqls.getValue().getRow());
                    logger.info(INFO_MARKER, "Phase 1/2 Table {}/{} Row {}/{}: Error\r\n{}", sel.getOrdNum(), tableNums,
                            entitySqls.getValue().getRow(),
                            entitySqls.getValue().getString(SelectQuery.TOTAL_ROWS_COL_NAME),
                            entitySql.getSqlError().getErrMsg());
                    logSqlError(entitySql.getSqlError());
                    continue;
                }

                if (StringUtils.isBlank(entitySql.getValue())) {
                    logger.info(INFO_MARKER,
                            "Phase 1/2 Table {}/{} Row {}/{}: Skip because a value with SELECT is blank",
                            sel.getOrdNum(), tableNums,
                            entitySqls.getValue().getRow(),
                            entitySqls.getValue().getString(SelectQuery.TOTAL_ROWS_COL_NAME));
                    continue;
                }

                // Remove unavailable statements of SELECT
                String selectSql = new String(entitySql.getValue());
                if (selectQuery.valueByName("IMP_DATA_TYPE_PARAM").getTableName().equalsIgnoreCase(sel.getTableName())) {
                    selectSql = replaceStaticImpDataTypeParam(selectSql);
                } else if (selectQuery.valueByName("IMP_ENTITY").getTableName().equalsIgnoreCase(sel.getTableName())
                        && isPlsqlBlock(selectSql)) {
                    logger.info(INFO_MARKER, "Phase 1/2 Table {}/{} Row {}/{}: Skip because it is PLSQL\r\n{}",
                            sel.getOrdNum(), tableNums,
                            entitySqls.getValue().getRow(),
                            entitySqls.getValue().getString(SelectQuery.TOTAL_ROWS_COL_NAME),
                            selectSql);
                    continue;
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
                    sqlErr.setPhase(1);
                    sqlErr.setTable(sel.getOrdNum());
                    sqlErr.setRow(entitySqls.getValue().getRow());
                    logger.info(INFO_MARKER, "Phase 1/2 Table {}/{} Row {}/{}: Error\r\n{}", sel.getOrdNum(), tableNums,
                            entitySqls.getValue().getRow(),
                            entitySqls.getValue().getString(SelectQuery.TOTAL_ROWS_COL_NAME),
                            sqlErr.getErrMsg());
                    logSqlError(sqlErr);
                    continue;
                }
                selectSql = removeSemicolonAtTheEnd(selectSql);
                selectSql = replaceDateBindVars(selectSql);
                try {
                    selectSql = replaceNonDateBindVars(selectSql);
                } catch (SqlParsingException e) {
                    SqlError err = new SqlError("REPLACE-BIND-VARS");
                    err.setTableName(sel.getTableName());
                    err.setEntityIdColName(sel.getPrimKeyColName());
                    err.setSqlColName(sel.getSqlColName());
                    err.setEntityId(entityId.getValue());
                    err.setQuery(selectSql);
                    err.setOriginalQuery(entitySql.getValue());
                    err.setPhase(1);
                    err.setTable(sel.getOrdNum());
                    err.setRow(entitySqls.getValue().getRow());
                    err.setErrMsg("Can not parse a SELECT to replace bind variables\r\n" + selectSql);
                    logger.info(INFO_MARKER, "Phase 1/2 Table {}/{} Row {}/{}: {}", sel.getOrdNum(), tableNums,
                            entitySqls.getValue().getRow(),
                            entitySqls.getValue().getString(SelectQuery.TOTAL_ROWS_COL_NAME),
                            err.getErrMsg());
                    logSqlError(err);
                    continue;
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
                    isSelectResult.getSqlError().setPhase(1);
                    isSelectResult.getSqlError().setTable(sel.getOrdNum());
                    isSelectResult.getSqlError().setRow(entitySqls.getValue().getRow());
                    logger.info(INFO_MARKER, "Phase 1/2 Table {}/{} Row {}/{}: Error\r\n{}", sel.getOrdNum(), tableNums,
                            entitySqls.getValue().getRow(),
                            entitySqls.getValue().getString(SelectQuery.TOTAL_ROWS_COL_NAME),
                            isSelectResult.getSqlError().getErrMsg());
                    logSqlError(isSelectResult.getSqlError());
                    continue;
                }

                logger.info(INFO_MARKER, "Phase 1/2 Table {}/{} Row {}/{}: OK", sel.getOrdNum(), tableNums,
                        entitySqls.getValue().getRow(),
                        entitySqls.getValue().getString(SelectQuery.TOTAL_ROWS_COL_NAME));
            }
            if (isEmptyTable) {
                logger.info(INFO_MARKER, "Phase 1/2 Table {}/{} Row 0/0: Table {} is empty", sel.getOrdNum(), tableNums,
                        sel.getTableName());
            }
        }
        if (dropView) {
            if (config.isUseSecondTest()) {
                try {
                    test2JdbcTemplate.update(DROP_SELECT_VIEW);
                } catch (DataAccessException e) {
                    logger.info(INFO_MARKER, "Phase 1/2: Error when Test2 view is deleting\r\n{}", e.getMessage());
                }
            } else {
                try {
                    test1JdbcTemplate.update(DROP_SELECT_VIEW);
                } catch (DataAccessException e) {
                    logger.info(INFO_MARKER, "Phase 1/2: Error when Test2 view is deleting\r\n{}", e.getMessage());
                }
            }
        }
    }

    private TableValue<Boolean> isSelectStatement(Configuration config, String selectSql) {
        String viewDdl = wrapSelectAsView(selectSql);

        boolean viewCreated = false;
        SqlError sqlErr = null;
        if (config.isUseSecondTest()) {
            try {
                test2JdbcTemplate.update(viewDdl);
                viewCreated = true;
            } catch (DataAccessException e2) {
                sqlErr = new SqlError("CREATE-VIEW2");
                sqlErr.setErrMsg(e2.getMessage());
            }
        } else {
            try {
                test1JdbcTemplate.update(viewDdl);
                viewCreated = true;
            } catch (DataAccessException e) {
                sqlErr = new SqlError("CREATE-VIEW1");
                sqlErr.setErrMsg(e.getMessage());
            }
        }
        if (viewCreated) {
            if (config.isUseSecondTest()) {
                SqlRowSet errSqlRowSet = test2JdbcTemplate.queryForRowSet(FIND_DB_OBJECT_ERRORS, SELECT_VIEW_NAME);
                if (errSqlRowSet.next()) {
                    TableValue<String> errResult = TableValue.createString(errSqlRowSet, "text");
                    if (errResult.hasError()) {
                        sqlErr = new SqlError("GET-VIEW-ERR2");
                        sqlErr.setErrMsg(errResult.getSqlError().getErrMsg());
                        viewCreated = false;
                    } else {
                        sqlErr = new SqlError("VIEW-ERR2");
                        sqlErr.setErrMsg(errResult.getValue());
                        viewCreated = false;
                    }
                }
            } else {
                SqlRowSet errSqlRowSet = test1JdbcTemplate.queryForRowSet(FIND_DB_OBJECT_ERRORS, SELECT_VIEW_NAME);
                if (errSqlRowSet.next()) {
                    TableValue<String> errResult = TableValue.createString(errSqlRowSet, "text");
                    if (errResult.hasError()) {
                        sqlErr = new SqlError("GET-VIEW-ERR1");
                        sqlErr.setErrMsg(errResult.getSqlError().getErrMsg());
                        viewCreated = false;
                    } else {
                        sqlErr = new SqlError("VIEW-ERR1");
                        sqlErr.setErrMsg(errResult.getValue());
                        viewCreated = false;
                    }
                }
            }
        }
        return new TableValue<Boolean>(viewCreated, sqlErr);
    }

    private void logSqlError(SqlError sqlError) {
        logger.info(DATA_MARKER, "{}", sqlError.toString());
        sqlErrors.add(sqlError);
    }

    private void testPlsqlBlocks(Configuration config) {
        boolean dropProc = false;
        int tableNums = /*plsqlBlock*/selectQuery.values().size();
        for (TableNode plsql : /*plsqlBlock*/selectQuery.values()) {
            /*if (config.isSkippedPlsqlTable(plsql.getTableName())) {
                logger.info(INFO_MARKER, "Phase 2/2 Table {}/{} is skipped", plsql.getOrdNum(), tableNums);
                continue;
            }*/

            TableValue<SqlRowSet> entitySqls = getSqlRowSetData(plsql);
            if (entitySqls.hasError()) {
                logger.info(INFO_MARKER,
                        "Phase 2/2 Table {}/{}: Error to get of list with PLSQL blocks for {}.{}\r\n{}",
                        plsql.getOrdNum(), tableNums, plsql.getTableName(), plsql.getSqlColName(),
                        entitySqls.getSqlError().toString());
                continue;
            }

            boolean isEmptyTable = true;
            while (entitySqls.getValue().next()) {
                isEmptyTable = false;

                TableValue<String> entityId = TableValue.createString(entitySqls.getValue(), plsql.getPrimKeyColName());
                if (entityId == null || entityId.hasError()) {
                    entityId.getSqlError().setTableName(plsql.getTableName());
                    entityId.getSqlError().setEntityIdColName(plsql.getPrimKeyColName());
                    entityId.getSqlError().setSqlColName(plsql.getSqlColName());
                    entityId.getSqlError().setEntityId(null);
                    entityId.getSqlError().setPhase(2);
                    entityId.getSqlError().setTable(plsql.getOrdNum());
                    entityId.getSqlError().setRow(entitySqls.getValue().getRow());
                    logger.info(INFO_MARKER, "Phase 2/2 Table {}/{} Row {}/{}: Error\r\n{}", plsql.getOrdNum(),
                            tableNums,
                            entitySqls.getValue().getRow(),
                            entitySqls.getValue().getString(SelectQuery.TOTAL_ROWS_COL_NAME),
                            entityId.getSqlError().getErrMsg());
                    continue;
                }

                TableValue<String> entityBlock = TableValue.createString(entitySqls.getValue(), plsql.getSqlColName());
                if (entityBlock == null || entityBlock.hasError()) {
                    entityBlock.getSqlError().setTableName(plsql.getTableName());
                    entityBlock.getSqlError().setEntityIdColName(plsql.getPrimKeyColName());
                    entityBlock.getSqlError().setSqlColName(plsql.getSqlColName());
                    entityBlock.getSqlError().setEntityId(entityId.getValue());
                    entityBlock.getSqlError().setPhase(2);
                    entityBlock.getSqlError().setTable(plsql.getOrdNum());
                    entityBlock.getSqlError().setRow(entitySqls.getValue().getRow());
                    logger.info(INFO_MARKER, "Phase 2/2 Table {}/{} Row {}/{}: Error\r\n{}", plsql.getOrdNum(),
                            tableNums,
                            entitySqls.getValue().getRow(),
                            entitySqls.getValue().getString(SelectQuery.TOTAL_ROWS_COL_NAME),
                            entityBlock.getSqlError().getErrMsg());
                    logSqlError(entityBlock.getSqlError());
                    continue;
                }

                if (StringUtils.isBlank(entityBlock.getValue())) {
                    logger.info(INFO_MARKER,
                            "Phase 2/2 Table {}/{} Row {}/{}: Skip because a value with PLSQL is blank",
                            plsql.getOrdNum(), tableNums,
                            entitySqls.getValue().getRow(),
                            entitySqls.getValue().getString(SelectQuery.TOTAL_ROWS_COL_NAME));
                    continue;
                }

                if (isSelectStatement(entityBlock.getValue())) {
                    // In some cases, the table column can contain PLSQL blocks and SELECT statements
                    continue;
                }

                String plsqlBlock = wrapBeginEndIfNeed(entityBlock.getValue());
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
                    err.setPhase(2);
                    err.setTable(plsql.getOrdNum());
                    err.setRow(entitySqls.getValue().getRow());
                    err.setErrMsg(e.getMessage());
                    logger.info(INFO_MARKER, "Phase 2/2 Table {}/{} Row {}/{}: Error\r\n{}\r\n{}", plsql.getOrdNum(),
                            tableNums,
                            entitySqls.getValue().getRow(),
                            entitySqls.getValue().getString(SelectQuery.TOTAL_ROWS_COL_NAME),
                            e.getMessage(), plsqlBlock);
                    logSqlError(err);
                    continue;
                }

                TableValue<Boolean> plsqlBlockResult = isPlsqlBlock(config, plsqlBlock);
                dropProc = true;
                if (plsqlBlockResult.hasError()) {
                    plsqlBlockResult.getSqlError().setTableName(plsql.getTableName());
                    plsqlBlockResult.getSqlError().setEntityIdColName(plsql.getPrimKeyColName());
                    plsqlBlockResult.getSqlError().setSqlColName(plsql.getSqlColName());
                    plsqlBlockResult.getSqlError().setEntityId(entityId.getValue());
                    plsqlBlockResult.getSqlError().setQuery(plsqlBlock);
                    plsqlBlockResult.getSqlError().setOriginalQuery(entityBlock.getValue());
                    plsqlBlockResult.getSqlError().setPhase(2);
                    plsqlBlockResult.getSqlError().setTable(plsql.getOrdNum());
                    plsqlBlockResult.getSqlError().setRow(entitySqls.getValue().getRow());
                    logger.info(INFO_MARKER, "Phase 2/2 Table {}/{} Row {}/{}: Error\r\n{}\r\n{}", plsql.getOrdNum(),
                            tableNums,
                            entitySqls.getValue().getRow(),
                            entitySqls.getValue().getString(SelectQuery.TOTAL_ROWS_COL_NAME),
                            plsqlBlockResult.getSqlError().getErrMsg(), plsqlBlock);
                    logSqlError(plsqlBlockResult.getSqlError());
                    continue;
                }

                logger.info(INFO_MARKER, "Phase 2/2 Table {}/{} Row {}/{}: OK", plsql.getOrdNum(), tableNums,
                        entitySqls.getValue().getRow(),
                        entitySqls.getValue().getString(SelectQuery.TOTAL_ROWS_COL_NAME));
            }
            if (isEmptyTable) {
                logger.info(INFO_MARKER, "Phase 2/2 Table {}/{} Row 0/0: Table is empty", plsql.getOrdNum(), tableNums);
            }
        }

        if (dropProc) {
            if (config.isUseSecondTest()) {
                try {
                    test2JdbcTemplate.update(DROP_PLSQL_PROC);
                } catch (DataAccessException e) {
                    logger.info(INFO_MARKER, "Phase 2/2 Test 2 Deleting procedure error [{}]", e.getMessage());
                }
            } else {
                try {
                    test1JdbcTemplate.update(DROP_PLSQL_PROC);
                } catch (DataAccessException e) {
                    logger.info(INFO_MARKER, "Phase 2/2 Test 1 Deleting procedure error [{}]", e.getMessage());
                }
            }
        }
    }

    private TableValue<Boolean> isPlsqlBlock(Configuration config, String plsqlBlock) {
        String procDdl = wrapBlockAsProc(plsqlBlock);

        boolean procCreated = false;
        SqlError sqlErr = null;
        if (config.isUseSecondTest()) {
            try {
                test2JdbcTemplate.update(procDdl);
                procCreated = true;
            } catch (DataAccessException e2) {
                sqlErr = new SqlError("CREATE-PROC2");
                sqlErr.setErrMsg(e2.getMessage());
            }
        } else {
            try {
                test1JdbcTemplate.update(procDdl);
                procCreated = true;
            } catch (DataAccessException e) {
                sqlErr = new SqlError("CREATE-PROC1");
                sqlErr.setErrMsg(e.getMessage());
            }
        }

        if (procCreated) {
            if (config.isUseSecondTest()) {
                SqlRowSet errSqlRowSet = test2JdbcTemplate.queryForRowSet(FIND_DB_OBJECT_ERRORS, PLSQL_PROC_NAME);
                if (errSqlRowSet.next()) {
                    TableValue<String> errResult = TableValue.createString(errSqlRowSet, "text");
                    if (errResult.hasError()) {
                        sqlErr = new SqlError("GET-PROC-ERR2");
                        sqlErr.setErrMsg(errResult.getSqlError().getErrMsg());
                        procCreated = false;
                    } else {
                        sqlErr = new SqlError("PROC-ERR2");
                        sqlErr.setErrMsg(errResult.getValue());
                        procCreated = false;
                    }
                }
            } else {
                SqlRowSet errSqlRowSet = test1JdbcTemplate.queryForRowSet(FIND_DB_OBJECT_ERRORS, PLSQL_PROC_NAME);
                if (errSqlRowSet.next()) {
                    TableValue<String> errResult = TableValue.createString(errSqlRowSet, "text");
                    if (errResult.hasError()) {
                        sqlErr = new SqlError("GET-PROC-ERR1");
                        sqlErr.setErrMsg(errResult.getSqlError().getErrMsg());
                        procCreated = false;
                    } else {
                        sqlErr = new SqlError("PROC-ERR1");
                        sqlErr.setErrMsg(errResult.getValue());
                        procCreated = false;
                    }
                }
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
            sql = replaceImpEntityParamsByEntityId(sql, entityId);
        } else if ("imp_spec".equalsIgnoreCase(tableName)) {
            sql = replaceImpSpecExtProcParams(sql);
        } else if ("rule".equalsIgnoreCase(tableName) && "sql_text".equalsIgnoreCase(sqlColName)) {
            sql = replaceRuleParams(sql, entityId);
        } else if ("wf_template_step".equalsIgnoreCase(tableName) || "wf_step".equalsIgnoreCase(tableName)) {
            sql = replaceWfStepParams(sql);
        }

        sql = replaceDateBindVars(sql);
        sql = replaceNonDateBindVars(sql);
        return sql;
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
        return sql;
    }

    private TableValue<SqlRowSet> getSqlRowSetData(CheckSqlQuery query) {
        SqlRowSet sqlRowSet = null;
        SqlError sqlErr = null;
        try {
            sqlRowSet = owner1JdbcTemplate.queryForRowSet(query.getSql());
        } catch (DataAccessException e1) {
            sqlRowSet = null;
            sqlErr = new SqlError(query.getQueryType() + "-ENTITY");
            sqlErr.setErrMsg(e1.getMessage());

        }
        return new TableValue<SqlRowSet>(sqlRowSet, sqlErr);
    }

}
