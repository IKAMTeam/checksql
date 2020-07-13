
package com.onevizion.checksql.support;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;

import com.onevizion.checksql.exception.UnexpectedException;
import com.onevizion.checksql.vo.AppSettings;

import oracle.ucp.jdbc.LabelableConnection;

public class ConnectionLabelingCallback implements oracle.ucp.jdbc.ConnectionLabelingCallback {

    @Autowired
    private AppSettings appSettings;

    @Override
    public boolean configure(Properties requestedLabels, Object connObj) {
        boolean success = false;
        Statement statement = null;
        try {
            Connection conn = (Connection) connObj;
            StringBuilder sql = new StringBuilder("call pkg_sec.set_pid(");
            if (appSettings.getTest1Schema().equalsIgnoreCase(conn.getMetaData().getUserName())) {
                sql.append(requestedLabels.getProperty("test1Pid"));
            } else if (appSettings.getTest2Schema().equalsIgnoreCase(conn.getMetaData().getUserName())) {
                sql.append(requestedLabels.getProperty("test2Pid"));
            } else {
                throw new UnexpectedException("Can not define what DB schema is been using");
            }
            sql.append(")");

            statement = conn.createStatement();
            statement.executeUpdate(sql.toString());

            LabelableConnection labelableConnection = (LabelableConnection) connObj;

            if (requestedLabels.getProperty("test1Pid") == null) {
                labelableConnection.removeConnectionLabel("test1Pid");
            } else {
                labelableConnection.applyConnectionLabel("test1Pid", requestedLabels.getProperty("test1Pid"));
            }

            if (requestedLabels.getProperty("test2Pid") == null) {
                labelableConnection.removeConnectionLabel("test2Pid");
            } else {
                labelableConnection.applyConnectionLabel("test2Pid", requestedLabels.getProperty("test2Pid"));
            }

            success = true;

        } catch (SQLException e) {
            success = false;

        } finally {
            try {
                if (statement != null && !statement.isClosed()) {
                    statement.close();
                }
            } catch (SQLException e1) {

            }
        }
        return success;
    }

    @Override
    public int cost(Properties requestedLabels, Properties currentLabels) {
        if (requestedLabels.equals(currentLabels)) {
            return 0;
        }
        return 100;
    }

    @Override
    public Properties getRequestedLabels() {
        Properties labels = new Properties();
        try {
            if (appSettings != null && appSettings.getTest1Pid() != null) {
                labels.setProperty("test1Pid", appSettings.getTest1Pid().toString());
            }
            if (appSettings != null && appSettings.getTest2Pid() != null) {
                labels.setProperty("test2Pid", appSettings.getTest2Pid().toString());
            }
        } catch (BeanCreationException e) {

        }
        return labels;
    }

}
