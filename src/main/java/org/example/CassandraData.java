package org.example;

import java.util.ArrayList;

public class CassandraData {
    private String tableName;
    private ArrayList<String> columnNames = new ArrayList<String>();
    public CassandraData(String tn, ArrayList<String> cn){
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