package org.example;

import java.util.ArrayList;

public class CassandraTable {
    private String tableName;
    private ArrayList<String> columnNames = new ArrayList<String>();
    public CassandraTable(String tn){
        tableName=tn;
    }
    public String getTableName(){
        return tableName;
    }
}
