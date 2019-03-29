package com.onevizion.checksql.vo;

import org.apache.commons.io.IOUtils;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.io.IOException;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;

public class TableValue<V> {

    private V value;
    private SqlError sqlError;

    public TableValue(SqlError sqlError) {
        this(null, sqlError);
    }

    public TableValue(V value) {
        this(value, null);
    }

    public TableValue(V value, SqlError sqlError) {
        this.value = value;
        this.sqlError = sqlError;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public SqlError getSqlError() {
        return sqlError;
    }

    public void setSqlError(SqlError sqlError) {
        this.sqlError = sqlError;
    }

    public static TableValue<String> createString(SqlRowSet sqlRowSet, String colName) {
        String strVal = null;
        SqlError sqlError = null;
        for (int colNum = 1; colNum <= sqlRowSet.getMetaData().getColumnCount(); colNum++) {
            if (!colName.equalsIgnoreCase(sqlRowSet.getMetaData().getColumnName(colNum))) {
                continue;
            }
            int colType = sqlRowSet.getMetaData().getColumnType(colNum);

            if (Types.CLOB == colType) {
                Clob clobObj = (Clob) sqlRowSet.getObject(colNum);

                try {
                    strVal = IOUtils.toString(clobObj.getCharacterStream());
                } catch (SQLException e) {
                    sqlError = new SqlError("Clob2Stream");
                    sqlError.setErrMsg(e.getMessage());
                } catch (IOException e) {
                    sqlError = new SqlError("Stream2Writer");
                    sqlError.setErrMsg(e.getMessage());
                }
            } else {
                strVal = sqlRowSet.getString(colNum);
            }

        }
        return new TableValue<>(strVal, sqlError);
    }

    public static TableValue<String> createStringErr(SqlRowSet sqlRowSet, boolean isPlSqlBlock) {
        StringBuilder strVal = new StringBuilder();
        SqlError sqlError = null;
        for (int colNum = 1; colNum <= sqlRowSet.getMetaData().getColumnCount(); colNum++) {
            if ("line".equalsIgnoreCase(sqlRowSet.getMetaData().getColumnName(colNum))) {
                strVal.append(", line ");
                Integer line = sqlRowSet.getInt(colNum);
                if (isPlSqlBlock) {
                    line -= 3;
                } else {
                    line -= 1;
                }
                strVal.append(line.toString());
                continue;
            }

            int colType = sqlRowSet.getMetaData().getColumnType(colNum);

            if (Types.CLOB == colType) {
                Clob clobObj = (Clob) sqlRowSet.getObject(colNum);

                try {
                    strVal.append(IOUtils.toString(clobObj.getCharacterStream()));
                } catch (SQLException e) {
                    sqlError = new SqlError("Clob2Stream");
                    sqlError.setErrMsg(e.getMessage());
                } catch (IOException e) {
                    sqlError = new SqlError("Stream2Writer");
                    sqlError.setErrMsg(e.getMessage());
                }
            } else {
                strVal.append(sqlRowSet.getString(colNum));
            }

        }
        return new TableValue<>(strVal.toString(), sqlError);
    }

    public boolean hasError() {
        return sqlError != null;
    }

}
