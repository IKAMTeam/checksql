//package com.onevizion.checksql.vo;
//
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
//import org.json.simple.parser.JSONParser;
//import org.json.simple.parser.ParseException;
//
//public class PlsqlBlock{
//
//	public static final String TOTAL_ROWS_COL_NAME = "totalrows";
//	private static final String FILENAME = "structure_test.json";
//
//	private List<TableNode> values = new ArrayList();
//	
//	public PlsqlBlock() {
//        
//        JSONParser parser = new JSONParser();
//        try {
//            JSONObject object = (JSONObject) parser.parse(
//                    new FileReader(FILENAME));
//            JSONArray messages = (JSONArray) object.get("plsql");
//            //System.out.println("Messages: " + messages);
//            for (int i = 0; i < messages.size(); i = i + 1) {
//                JSONObject tempobject = (JSONObject) messages.get(i);
//                int ordNum = i + 1;
//                String tempstr = tempobject.keySet().toString().substring(1, tempobject.keySet().toString().length()-1);
//                String tableName = tempstr.substring(0, tempstr.indexOf('.'));
//                String primKeyColName = tempstr.substring(tempstr.indexOf('.')+1);
//                tempstr = tempobject.values().toString();
//                String whereClause = tempstr.substring(tempstr.indexOf("\"whereClause\":") + 14, tempstr.indexOf("\"fromClause\":")-1);
//                if (!whereClause.equals("null")){
//                    whereClause = whereClause.substring(1, whereClause.length()-1);
//                }
//                else{
//                    whereClause = null;
//                }
//                String fromClause = tempstr.substring(tempstr.indexOf("\"fromClause\":") + 13, tempstr.indexOf("\"sqlColName\":")-1);
//                if (!fromClause.equals("null")){
//                    fromClause = fromClause.substring(1, fromClause.length()-1);
//                }
//                else{
//                    fromClause = null;
//                }
//                String sqlColName = tempstr.substring(tempstr.indexOf("\"sqlColName\":") + 13, tempstr.length()-2);
//                if (!sqlColName.equals("null")){
//                    sqlColName = sqlColName.substring(1, sqlColName.length()-1);
//                }
//                else{
//                    sqlColName = null;
//                }
//                TableNode tempr = new TableNode(ordNum, tableName.toLowerCase(), fromClause, sqlColName, primKeyColName.toLowerCase(), whereClause, "PLSQL", this.TOTAL_ROWS_COL_NAME);
//                this.values.add(tempr);
//                /*System.out.println("Element: " + this.values.size() + " " + 
//                        this.values.get(this.values.size()-1).getOrdNum() + " " + this.values.get(this.values.size()-1).getTableName() + " " + 
//                        this.values.get(this.values.size()-1).getPrimKeyColName() + " " + this.values.get(this.values.size()-1).getWhereClause() + " " + 
//                        this.values.get(this.values.size()-1).getFromClause() + " " + this.values.get(this.values.size()-1).getSqlColName());*/
//            }            
//        } catch (IOException | ParseException ex) {
//            Logger.getLogger(PlsqlBlock.class.getName())
//                    .log(Level.SEVERE, null, ex);
//        }
//
//    }
//    
//    public List<TableNode> values(){
//        return this.values;
//    }
//    
//    public TableNode valueByName(String name) throws Exception{
//        int id = -1;
//        for (TableNode ff : this.values()){
//            if (ff.getTableName().equals(name.toLowerCase())){
//                id = ff.getOrdNum() - 1;
//            }
//        }
//        if (id == -1){
//            throw new Exception("PlsqlBlock.valueByName(String name) TableNode with this name not found");
//        }
//        else {
//            return this.values.get(id);
//        } 
//    }
//    
//}
