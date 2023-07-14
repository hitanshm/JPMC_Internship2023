package org.example;

import com.datastax.driver.core.Session;

public class Insertvalue {
    private String tableName;
    private String keyspace;
    private String column1;
    private String column2;
    private String id;
    private String name;
    private Session session;

    public Insertvalue(Session session, String keyspace, String tableName, String column1, String column2, String id, String name){
        this.keyspace=keyspace;
        this.tableName=tableName;
        this.column1=column1;
        this.column2=column2;
        this.id=id;
        this.name=name;
        this.session = session;
    }
    public void insertTable() {
        StringBuilder sb = new StringBuilder("insert into ").append(keyspace).append(".").append(tableName)
                .append(" (").append(column1).append(",").append(column2).append(") values (")
                .append(id).append(",'").append(name).append("') IF NOT EXISTS;");

        final String query = sb.toString();

        session.execute(query);
    }
}