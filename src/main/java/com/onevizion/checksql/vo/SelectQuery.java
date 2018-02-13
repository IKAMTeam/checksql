package com.onevizion.checksql.vo;

import com.onevizion.checksql.exception.UnexpectedException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Resource;
import oracle.ucp.jdbc.PoolDataSourceImpl;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import com.onevizion.checksql.vo.AppSettings;

public class SelectQuery{

	public static final String TOTAL_ROWS_COL_NAME = "totalrows";
	private static final String FILENAME = "checksql.json";
//        @Resource(name = "owner1JdbcTemplate")
//        private JdbcTemplate owner1JdbcTemplate;
//        @Resource(name = "test1JdbcTemplate")
//        private JdbcTemplate test1JdbcTemplate;
//        @Resource(name = "test2JdbcTemplate")
//        private JdbcTemplate test2JdbcTemplate;
//        @Resource(name = "owner2JdbcTemplate")
//        private JdbcTemplate owner2JdbcTemplate;
//        private AppSettings appSettings;
//        private static final String FIND_FIRST_PROGRAM_ID_NEW = "select program_id from program where rownum < 2 and program_id <> 0";
//        private static final String FIND_FIRST_PROGRAM_ID_OLD = "select program_id from v_program where rownum < 2";
    
    private List<TableNode> values = new ArrayList();

    public SelectQuery(JdbcTemplate owner1JdbcTemplate) {
        
        JSONParser parser = new JSONParser();
        try {
            
            JSONArray message = (JSONArray) parser.parse(
                new FileReader(FILENAME));
            
            String temp = message.toString();
            String sqlquery, table, column, whereClause, fromClause;
            int n = message.size();
            int k;
            for (int i = 0; i < n; i = i + 1){
                temp = message.get(i).toString();
                Object object = message.get(i);
                temp = object.toString();
                k = temp.indexOf("\"");
                if (k != -1){
                    JSONObject jsonobject = (JSONObject) message.get(i);
                    sqlquery = jsonobject.values().toString().substring(1, jsonobject.values().toString().length() - 1);
                    whereClause = sqlquery.substring(sqlquery.toLowerCase().indexOf("where") + 6);
                    fromClause = sqlquery.substring(sqlquery.toLowerCase().indexOf("from") + 4, sqlquery.toLowerCase().indexOf("where"));
                    temp = temp.substring(2, temp.indexOf(":") - 1);
                    table = temp.substring(0, temp.indexOf("."));
                    column = temp.substring(temp.indexOf(".") + 1);
                } else{
                    table = temp.substring(0, temp.indexOf("."));
                    column = temp.substring(temp.indexOf(".") + 1);
                    sqlquery = "select " + column + " from " + table;
                    whereClause = null;
                    fromClause = table;
                }
                
                String primKeyColName = "primKeyColName";
                Exception e = null;
                try {
                    primKeyColName = owner1JdbcTemplate.queryForObject("SELECT COLUMN_NAME FROM ALL_CONS_COLUMNS WHERE CONSTRAINT_NAME IN ( SELECT CONSTRAINT_NAME FROM ALL_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'P' and TABLE_NAME = '" + table + "' ) AND ROWNUM = 1", String.class);    
                } catch (DataAccessException e1) {
                    e = e1;
                }
            
                TableNode tempr = new TableNode(i + 1, table.toLowerCase(), fromClause, column, primKeyColName.toLowerCase(), whereClause, "SQL", this.TOTAL_ROWS_COL_NAME);
                this.values.add(tempr);
            }            
        } catch (IOException | ParseException ex) {
            Logger.getLogger(SelectQuery.class.getName())
                    .log(Level.SEVERE, null, ex);
        }

    }
    
//    private void configAppSettings(Configuration config) {
//        appSettings.setTest1Pid(getRandomTest1Pid());
//
//        PoolDataSourceImpl test1DataSource = (PoolDataSourceImpl) test1JdbcTemplate.getDataSource();
//        appSettings.setTest1Schema(test1DataSource.getUser());
//
//        if (config.isUseSecondTest()) {
//            appSettings.setTest2Pid(getRandomTest2Pid());
//
//            PoolDataSourceImpl test2DataSource = (PoolDataSourceImpl) test2JdbcTemplate.getDataSource();
//            appSettings.setTest2Schema(test2DataSource.getUser());
//        }
//    }
//    private Long getRandomTest1Pid() {
//        Long pid = null;
//        Exception e = null;
//        try {
//            pid = owner1JdbcTemplate.queryForObject(FIND_FIRST_PROGRAM_ID_OLD, Long.class);         
//        } catch (DataAccessException e1) {
//            e = e1;
//        }
//
//        if (pid == null) {
//            try {
//                pid = owner1JdbcTemplate.queryForObject(FIND_FIRST_PROGRAM_ID_NEW, Long.class);
//            } catch (DataAccessException e1) {
//                e = e1;
//            }
//        }
//
//        if (pid == null) {
//            throw new UnexpectedException("[Test1] Can not get a PROGRAM_ID", e);
//        }
//        return pid;
//    }
//
//    private Long getRandomTest2Pid() {
//        Long pid = null;
//        Exception e = null;
//        try {
//            pid = owner2JdbcTemplate.queryForObject(FIND_FIRST_PROGRAM_ID_OLD, Long.class);
//        } catch (DataAccessException e1) {
//            e = e1;
//        }
//
//        if (pid == null) {
//            try {
//                pid = owner2JdbcTemplate.queryForObject(FIND_FIRST_PROGRAM_ID_NEW, Long.class);
//            } catch (DataAccessException e1) {
//                e = e1;
//            }
//        }
//
//        if (pid == null) {
//            throw new UnexpectedException("[Test2] Can not get a PROGRAM_ID", e);
//        }
//        return pid;
//    }
    
    public List<TableNode> values(){
        return this.values;
    }

    public TableNode valueByName(String name) throws Exception{
        int id = -1;
        //String where = "where", from = "from", pk = "pk", sqlcol = "sqlcol", sql = "sql";
        for (TableNode ff : this.values()){
            if (ff.getTableName().equals(name.toLowerCase())){
                id = ff.getOrdNum() - 1;
                //where = ff.getWhereClause();
                //from = ff.getFromClause();
                //pk = ff.getPrimKeyColName();
                //sqlcol = ff.getSqlColName();
                //sql = ff.getSql();
            }
        }
        //id = -1;
        if (id == -1){
            throw new Exception("SelectQuery.valueByName(String " + name + ") TableNode with this name not found");
            //throw new Exception("name" + name + " where" + where + " from" + from + " pk" + pk + " sqlcol" + sqlcol + " sql" + sql + ") TableNode with this name not found");
        }
        else {
            return this.values.get(id);
        } 
    }
    
}
