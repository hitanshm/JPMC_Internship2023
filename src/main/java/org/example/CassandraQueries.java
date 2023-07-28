package org.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CassandraQueries {

    public static KeyspaceRepository schemaRepository;
    public static Session session;
    public static int amtOfTables;
    public static List<String> getAllColumnsFromTable(String tableName, String keyspaceName){
        System.out.println("Get all collumns from table " + tableName);
        String query = "SELECT * FROM " + keyspaceName + "." + tableName;

        System.out.println("About to execute query " + query);
        ResultSet result = session.execute(query);
        System.out.println("Finished execute query " + query);
        List<String> columnNames =
                result.getColumnDefinitions().asList().stream()
                        .map(cl -> cl.getName())
                        .collect(Collectors.toList());
        System.out.println(columnNames.toString());
        return columnNames;
    }

    public static List<Row> getAllRowsFromTable(String tableName, List<String> collumnNames, String keyspaceName){
        System.out.println("Get all rows from table " + tableName);
        String query = "SELECT * FROM " + keyspaceName + "." + tableName;
        //Creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        //Creating Session object
        Session session = cluster.connect("sample_demo");
        //Getting the ResultSet
        ResultSet result = session.execute(query);
        //Read data from ResultSet and convert into List and return as list of strings
        List<Row> allRowsData = new ArrayList<>();
        allRowsData = result.all();

        return allRowsData;


    }

    public static void whenCreatingAKeyspace_thenCreated() {

        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);
        session = client.getSession();
        schemaRepository = new KeyspaceRepository(session);

        String keyspaceName = "test4";

        System.out.println("Creating keyspace");

        schemaRepository.createKeyspace(keyspaceName, "SimpleStrategy", 1);

        System.out.println("Finished creating keyspace");

        ResultSet result =
                session.execute("SELECT * FROM system_schema.keyspaces;");

        List<String> matchedKeyspaces = result.all()
                .stream()
                .filter(r -> r.getString(0).equals(keyspaceName.toLowerCase()))
                .map(r -> r.getString(0))
                .collect(Collectors.toList());

        assertEquals(matchedKeyspaces.size(), 1);
        assertTrue(matchedKeyspaces.get(0).equals(keyspaceName.toLowerCase()));




        System.out.println("Process has ran successfully.");

    }
}
