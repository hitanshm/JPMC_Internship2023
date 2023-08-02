package org.example;

import com.datastax.driver.core.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Cassandra {
    private static String keyspaceName;
    private static String tableName;
    private Cluster cluster;
    private Session session;
    private CassandraTable table;
    private String user;
    private String password;

    //constructor
    public Cassandra(){

    }
    //constructor
    public Cassandra(String user, String password){
        this.user=user;
        this.password=password;
    }
    //constructor
    public Cassandra(Session session, CassandraTable table, String user, String password){
        this.session=session;
        this.table=table;
        this.user=user;
        this.password=password;
        keyspaceName=table.getKeyspaceName();
        tableName=table.getTableName();
    }
    //gets the ResultSet data of a table
    public Session getCassandraSession(CassandraTable table){
        if (session!=null){
            return session;
        }
        //Creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1")
                .withCredentials(user, password).build();
        //Creating Session object
        session = cluster.connect(table.getKeyspaceName());
        return session;
    }
    public ResultSet getResult(CassandraTable table){
        String query = "SELECT * FROM " + table.getKeyspaceName()+"."+ table.getTableName();
        session= getCassandraSession(table);
        //Getting the ResultSet
        ResultSet result = session.execute(query);
        return result;
    }
    //gets all data from a cassandra table
    public List<Row> getAllFromTable(CassandraTable table){

        ResultSet result = getResult(table);
        List<Row> allData=result.all();
        return allData;
    }
    //gets all column names in a cassandra table
    public List<String> getAllColumnsFromTable(CassandraTable table){

        ResultSet result = getResult(table);
        List<String> columnNames =
                result.getColumnDefinitions().asList().stream()
                        .map(cl -> cl.getName())
                        .collect(Collectors.toList());
        return columnNames;
    }
    //gets all tables within a keyspace in Cassandra
    public ArrayList<String> getTables(String keyspaceName){

        ArrayList<String> tableNames = new ArrayList<String>();
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1")
                .withCredentials(user,password).build();
        Metadata metadata = cluster.getMetadata();
        for (TableMetadata t : metadata.getKeyspace(keyspaceName).getTables()) {
            tableNames.add(t.getName());
        }
        int numOfTables = tableNames.size();
        System.out.println("Table names: " + tableNames);
        System.out.println("Total table count: " + numOfTables);

        return tableNames;
    }
}