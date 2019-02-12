package com.onevizion.checksql.vo;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SelectQuery{

    public static final String LINE_DELIMITER = "\r\n";
	public static final String TOTAL_ROWS_COL_NAME = "totalrows";
	private static final String FILENAME = "checksql.json";
    private static final String CHECKSQL_JSON_ERROR_MSG = "Create checksql.json file in working directory or set a path " +
            "to the file in arguments." + LINE_DELIMITER +
            LINE_DELIMITER +
            "Structure of checksql.json file:" +
            LINE_DELIMITER +
            "[" + LINE_DELIMITER +
            "  {\"table.column\": \"sql select statement\"}," + LINE_DELIMITER +
            "  \"table.column\"" + LINE_DELIMITER +
            "]";
    
    private List<TableNode> values = new ArrayList();

    public SelectQuery(JdbcTemplate owner1JdbcTemplate) {
        parseConfigFile(owner1JdbcTemplate, FILENAME);
    }

    public SelectQuery(JdbcTemplate owner1JdbcTemplate, String file) {
        parseConfigFile(owner1JdbcTemplate, file);
    }

    private void parseConfigFile(JdbcTemplate owner1JdbcTemplate, String file) {
        JSONParser parser = new JSONParser();
        try {

            JSONArray message = (JSONArray) parser.parse(
                    new FileReader(file));

            String temp = message.toString();
            String sqlquery, table, column, whereClause, fromClause;
            int n = message.size();
            int k;
            for (int i = 0; i < n; i = i + 1) {
                temp = message.get(i).toString();
                Object object = message.get(i);
                temp = object.toString();
                k = temp.indexOf("\"");
                if (k != -1) {
                    JSONObject jsonobject = (JSONObject) message.get(i);
                    sqlquery = jsonobject.values().toString().substring(1, jsonobject.values().toString().length() - 1);
                    whereClause = sqlquery.substring(sqlquery.toLowerCase().indexOf("where") + 6);
                    fromClause = sqlquery.substring(sqlquery.toLowerCase().indexOf("from") + 4, sqlquery.toLowerCase().indexOf("where"));
                    temp = temp.substring(2, temp.indexOf(":") - 1);
                    table = temp.substring(0, temp.indexOf("."));
                    column = temp.substring(temp.indexOf(".") + 1);
                } else {
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
                  .log(Level.SEVERE, CHECKSQL_JSON_ERROR_MSG, ex);
        }
    }
    
    public List<TableNode> values(){
        return this.values;
    }

    public TableNode valueByName(String name) throws Exception{
        int id = -1;
        for (TableNode ff : this.values()){
            if (ff.getTableName().equals(name.toLowerCase())){
                id = ff.getOrdNum() - 1;
            }
        }

        if (id == -1){
            throw new Exception("SelectQuery.valueByName(String " + name + ") TableNode with this name not found");
        }
        else {
            return this.values.get(id);
        } 
    }
    
}
