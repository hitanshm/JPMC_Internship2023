package org.example;

import java.util.ArrayList;

public class CassandraTable {
    private String tableName;
    private ArrayList<String> columnNames = new ArrayList<String>();
    public CassandraTable(String tn, ArrayList<String> cn){
        tableName=tn;
        columnNames=cn;
    }
    public String getTableName(){
        return tableName;
    }
    public ArrayList<String> getColumnNames(){
        return columnNames;
    }
}