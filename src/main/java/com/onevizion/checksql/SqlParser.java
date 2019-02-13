package com.onevizion.checksql;

import com.onevizion.checksql.exception.SqlParsingException;
import com.onevizion.checksql.vo.SqlStatementType;
import gudusoft.gsqlparser.*;
import gudusoft.gsqlparser.nodes.TOrderBy;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.nodes.TResultColumnList;
import gudusoft.gsqlparser.stmt.TCommonBlock;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlParser {

    private static Pattern orderByPattern = Pattern.compile("\\s+order\\s+by\\s*", Pattern.DOTALL);

    private static Pattern orderByInBracketsPattern = Pattern.compile("\\s+order\\s+by\\s*.*?\\)", Pattern.DOTALL);

    private static Pattern quotesPattern = Pattern.compile("'[^']*?'");

    public static List<String> getCols(String sqlText) {
        return getCols(getParser(sqlText));
    }

    public static List<String> getCols(TGSqlParser sqlParser) {
        TSelectSqlStatement select = (TSelectSqlStatement) sqlParser.sqlstatements.get(0);
        while (select.isCombinedQuery()) {
            select = select.getLeftStmt();
        }
        TResultColumnList columns = select.getResultColumnList();
        List<String> cols = new ArrayList<String>();
        for (int i = 0; i < columns.size(); i++) {
            TResultColumn cl = columns.getResultColumn(i);
            if (cl.getExpr().getExpressionType() == EExpressionType.function_t) {
                cols.add(cl.getExpr().toString().toUpperCase());
            } else {
                cols.add(cl.getExpr().getEndToken().toString().toUpperCase());
            }

        }
        return cols.size() == 0 ? null : cols;
    }

    public static List<String> getAliases(String sqlText) {
        return getAliases(getParser(sqlText));
    }

    public static List<String> getAliases(TGSqlParser sqlParser) {
        TSelectSqlStatement select = (TSelectSqlStatement) sqlParser.sqlstatements.get(0);
        while (select.isCombinedQuery()) {
            select = select.getLeftStmt();
        }
        TResultColumnList columns = select.getResultColumnList();

        List<String> cols = new ArrayList<String>();
        for (int i = 0; i < columns.size(); i++) {
            TResultColumn cl = columns.getResultColumn(i);
            if (cl.getAliasClause() != null) {
                cols.add(cl.getAliasClause().toString().replaceAll("\"", ""));
            } else {
                cols.add(cl.getEndToken().toString());
            }
        }

        return cols.size() == 0 ? null : cols;
    }

    public static List<String> getOriginalAliases(String sqlText) {
        return getOriginalAliases(getParser(sqlText));
    }

    public static List<String> getOriginalAliases(TGSqlParser sqlParser) {
        TSelectSqlStatement select = (TSelectSqlStatement) sqlParser.sqlstatements.get(0);
        while (select.isCombinedQuery()) {
            select = select.getLeftStmt();
        }
        TResultColumnList columns = select.getResultColumnList();

        List<String> cols = new ArrayList<String>();
        for (int i = 0; i < columns.size(); i++) {
            TResultColumn cl = columns.getResultColumn(i);
            if (cl.getAliasClause() != null) {
                cols.add(cl.getAliasClause().toString());
            } else {
                cols.add(cl.getEndToken().toString());
            }
        }

        return cols.size() == 0 ? null : cols;
    }

    public static List<String> getParams(String sqlText) {
        return getParams(getParser(sqlText));
    }

    public static List<String> getParams(TGSqlParser sqlParser) {
        Set<String> result = new HashSet<String>();
        for (int i = 0; i < sqlParser.sourcetokenlist.size(); i++) {
            TSourceToken st = sqlParser.sourcetokenlist.get(i);
            if (st.tokentype == ETokenType.ttbindvar) {
                result.add(st.toString().substring(1, st.toString().length()));
            }
        }
        return new ArrayList<String>(result);
    }

    public static boolean hasOrderByStatement(String sqlText) {
        TGSqlParser sqlParser = getParser(sqlText);
        TSelectSqlStatement select = (TSelectSqlStatement) sqlParser.sqlstatements.get(0);
        TOrderBy orderBy = select.getOrderbyClause();
        if (orderBy == null || orderBy.getItems() == null || orderBy.getItems().size() == 0) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Fast alternative to hasOrderByStatement method. It doesn't uses parser,
     * instead it counts occurrences of
     * "order by*)" and compare with count of occurrences of "order by"
     * 
     * @param sqlText
     * @return
     */
    public static boolean hasOrderByStatementFast(String sqlText) {
        if (!StringUtils.containsIgnoreCase(sqlText.toString(), "order by")) {
            return false;
        }

        int orderByInBracketCount = 0;
        int orderByInQuotesCount = 0;
        int orderByCount = 0;

        Matcher orderByInBracketsMatcher = orderByInBracketsPattern.matcher(sqlText);
        while (orderByInBracketsMatcher.find()) {
            if (!orderByInBracketsMatcher.group().contains("(")) { // make sure
                                                                   // this is
                                                                   // not order
                                                                   // by
                                                                   // function()
                orderByInBracketCount++;
            }
        }

        Matcher orderByMatcher = orderByPattern.matcher(sqlText);
        while (orderByMatcher.find()) {
            orderByCount++;
        }

        Matcher quotesMatcher = quotesPattern.matcher(sqlText);
        while (quotesMatcher.find()) {
            if (StringUtils.containsIgnoreCase(quotesMatcher.group(), "order by")) {
                orderByInQuotesCount++;
            }
        }

        return orderByCount > orderByInBracketCount + orderByInQuotesCount;
    }

    public static String setUpperBindVars(String sqlText) {
        TGSqlParser parser = getParser(sqlText);
        if (parser.sourcetokenlist != null) {
            for (int i = 0; i < parser.sourcetokenlist.size(); i++) {
                TSourceToken st = parser.sourcetokenlist.get(i);
                if (st.tokentype == ETokenType.ttbindvar) {
                    st.setString(st.toString().toUpperCase());
                }
            }
        }
        TSelectSqlStatement select = (TSelectSqlStatement) parser.sqlstatements.get(0);
        return select.toString();
    }

    public static boolean isPlsqlBlock(String sqlText) {
        boolean isPlsql = false;

        if (StringUtils.isNotBlank(sqlText)) {
            TGSqlParser sqlParser = getParser(sqlText);

            try {
                TCommonBlock block = (TCommonBlock) sqlParser.sqlstatements.get(0);
                TCustomSqlStatement body = block.getBodyStatements().get(0);
                isPlsql = StringUtils.isNotBlank(body.toString());
            } catch (ClassCastException s) {
                isPlsql = false;
            }
        }
        return isPlsql;
    }

    public static String getFirstTableName(String sqlText) {
        if (isSelectStatement(sqlText)) {
            TGSqlParser sqlParser = getParser(sqlText);
            TSelectSqlStatement select = (TSelectSqlStatement) sqlParser.sqlstatements.get(0);
            if (select.tables.size() > 0) {
                return select.tables.getTable(0).getName();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    public static String replaceBindVars(String sql, String replacedByVal) {
        TGSqlParser sqlParser = getParser(sql);
        for (int i = 0; i < sqlParser.sourcetokenlist.size(); i++) {
            TSourceToken st = sqlParser.sourcetokenlist.get(i);
            if (st.tokentype == ETokenType.ttbindvar) {
                st.setString("'0'");
            }
        }
        return sqlParser.sqlstatements.get(0).toString();
    }

    public static String removeIntoClause(String sql) {
        TGSqlParser parser = getParser(sql);
        List<String> tokens = new ArrayList<String>();
        boolean isIntoClause = false;
        for (int i = 0; i < parser.sourcetokenlist.size(); i++) {
            TSourceToken st = parser.sourcetokenlist.get(i);
            if (!isIntoClause) {
                if (st.tokentype != ETokenType.ttkeyword
                        || (st.tokentype == ETokenType.ttkeyword && !st.toString().equalsIgnoreCase("into"))) {
                    tokens.add(st.toString());
                } else {
                    isIntoClause = true;
                }
            } else {
                if (st.tokentype == ETokenType.ttkeyword && st.toString().equalsIgnoreCase("from")) {
                    tokens.add(st.toString());
                    isIntoClause = false;
                }
            }
        }
        String newSql = StringUtils.concatList(tokens, "");
        return newSql;
    }

    public static boolean isSelectStatement(String sqlText) {
        boolean isSelect = false;

        if (StringUtils.isNotBlank(sqlText)) {
            TGSqlParser sqlParser = getParser(sqlText);

            if (sqlParser.sqlstatements == null || sqlParser.sqlstatements.size() == 0) {
                isSelect = false;
            } else {
                TCustomSqlStatement customSqlStatement = sqlParser.sqlstatements.get(0);
                if (customSqlStatement instanceof TSelectSqlStatement) {
                    TSelectSqlStatement selectSqlStatement = (TSelectSqlStatement) customSqlStatement;
                    isSelect = StringUtils.isNotBlank(selectSqlStatement.toString());
                } else {
                    isSelect = false;
                }
            }
        }
        return isSelect;
    }


    public static TGSqlParser getParser(String sqlText) {
        TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
        sqlparser.sqltext = sqlText;
        int ret = sqlparser.parse();
        String msg = null;
        if (ret > 0) {
            ArrayList<TSyntaxError> errors = sqlparser.getSyntaxErrors();
            TSyntaxError error = errors.get(0);
            msg = "Syntax error near \"" + error.tokentext + "\", line " + error.lineNo + ", column " + error.columnNo;
        } else if (ret < 0) {
            msg = "General parser error: " + sqlparser.getErrormessage();
        }

        if (StringUtils.isNotBlank(msg)) {
            throw new SqlParsingException(msg);
        }

        return sqlparser;
    }

    public static SqlStatementType recognizeSelectStatementAndPlSql(String sqlText) {
        boolean selectStatementCheck = false;
        boolean plsqlBlockCheck = false;

        if (!sqlText.startsWith("declare") && !sqlText.startsWith("begin") && !sqlText.startsWith("if")) {
            try {
                selectStatementCheck = isSelectStatement(sqlText);
            } catch (Exception e) {
                selectStatementCheck = false;
            }
        } else {
            return SqlStatementType.PL_SQL;
        }

        if (selectStatementCheck) {
            return SqlStatementType.SELECT;
        }

        try {
            plsqlBlockCheck = isPlsqlBlock(sqlText);
        } catch (Exception e) {
            plsqlBlockCheck = false;
        }

        if (!plsqlBlockCheck) {
            String sqlTextWithBeginEnd = "begin " + sqlText + " end;";

            try {
                plsqlBlockCheck = isPlsqlBlock(sqlTextWithBeginEnd);
            } catch (Exception e) {
                plsqlBlockCheck = false;
            }
        }

        if (!plsqlBlockCheck) {
            return SqlStatementType.EMPTY;
        } else {
            return SqlStatementType.PL_SQL;
        }
    }

}
