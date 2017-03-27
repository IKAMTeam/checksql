package com.onevizion.checksql.vo;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.commons.io.IOUtils;
import org.springframework.jdbc.support.rowset.SqlRowSet;

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

                InputStream in;
                try {
                    in = clobObj.getAsciiStream();
                } catch (SQLException e3) {
                    sqlError = new SqlError("Clob2Stream");
                    sqlError.setErrMsg(e3.getMessage());
                    in = null;
                }

                if (in != null) {
                    StringWriter w = new StringWriter();
                    try {
                        IOUtils.copy(in, w);
                    } catch (IOException e3) {
                        sqlError = new SqlError("Stream2Writer");
                        sqlError.setErrMsg(e3.getMessage());
                        w = null;
                    }
                    if (w != null) {
                        strVal = w.toString();
                    }
                }
            } else {
                strVal = sqlRowSet.getString(colNum);
            }
        }
        return new TableValue<String>(strVal, sqlError);
    }

    public boolean hasError() {
        return sqlError != null;
    }

}
