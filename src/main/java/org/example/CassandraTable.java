package org.example;

import java.util.ArrayList;

public class CassandraTable {
    private String tableName;
    public CassandraTable(String tn){
        tableName=tn;
    }
    public String getTableName(){
        return tableName;
    }
}
