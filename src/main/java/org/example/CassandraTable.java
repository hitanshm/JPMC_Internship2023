package org.example;

import java.util.ArrayList;

public class CassandraTable {
    private String keyspaceName;
    private String tableName;
    public CassandraTable( String keyspaceName,String tableName){
        this.tableName=tableName;
        this.keyspaceName=keyspaceName;
    }
    public String getTableName(){
        return tableName;
    }
}
