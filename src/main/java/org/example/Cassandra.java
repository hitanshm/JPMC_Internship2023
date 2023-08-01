package org.example;

import com.datastax.driver.core.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class Cassandra {
    private String keyspaceName;
    private String tableName;
    private Cluster cluster;

    private Session session;
    public Cassandra(Session session){
        this.session=session;
    }
    //Connecting Cassandra
    public void connect(String node, Integer port, String user, String password) {
        Cluster.Builder b = Cluster.builder().addContactPoint(node).withCredentials(user, password);
        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();

        session = cluster.connect();
    }

    public Session getSession() {

        return this.session;
    }

    public void close() {
        session.close();
        cluster.close();
    }
    public static ResultSet getAllFromTable(CassandraTable table, String keyspace){

        String query = "SELECT * FROM " + table.getTableName();
        //Creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withCredentials("hitansh", "hitansh").build();
        //Creating Session object
        Session session = cluster.connect(keyspace);
        //Getting the ResultSet
        ResultSet result = session.execute(query);
        return result;
    }
    public static List<String> getAllColumnsFromTable(CassandraTable table, String keyspaceName){
        String query = "SELECT * FROM " + table.getTableName();
        //Creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withCredentials("hitansh", "hitansh").build();
        //Creating Session object
        Session session = cluster.connect(keyspaceName);
        //Getting the ResultSet
        ResultSet result = session.execute(query);
        List<String> columnNames =
                result.getColumnDefinitions().asList().stream()
                        .map(cl -> cl.getName())
                        .collect(Collectors.toList());
        return columnNames;
    }
    public static List getTables(String keyspaceName){

        List tableNames = new ArrayList();

        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withCredentials("hitansh", "hitansh").build();
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
