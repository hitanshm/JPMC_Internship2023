package org.example;

import java.util.ArrayList;

public class CassandraTable {
    private String keyspaceName;
    private String tableName;

    //constructor
    public CassandraTable(String keyspaceName,String tableName){
        this.tableName=tableName;
        this.keyspaceName=keyspaceName;
    }
    //returns table name
    public String getTableName(){
        return tableName;
    }
    //returns keyspace name
    public String getKeyspaceName() {
        return keyspaceName;
    }
}