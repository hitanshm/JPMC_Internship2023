package org.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.cassandra.config.Config.RequestSchedulerId.keyspace;

public class KeyspaceRepository {

    String keyspace1 = "sample_demo";

    private Session session;
    public KeyspaceRepository(Session session) {
        this.session = session;
    }
    public void createKeyspace(
            String keyspaceName, String replicationStrategy, int replicationFactor) {
        StringBuilder sb =
                new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                        .append(keyspaceName).append(" WITH replication = {")
                        .append("'class':'").append(replicationStrategy)
                        .append("','replication_factor':").append(replicationFactor)
                        .append("};");

        String query = sb.toString();
        session.execute(query);
    }

    /*
    public SampleTable selectRow(String tableName) {
        StringBuilder sb = new StringBuilder("select * from ").append(keyspace1).append(".").append(tableName).append(";");

        final String query = sb.toString();

        ResultSet result = session.execute(query);
        Row row = result.one();
        SampleTable ad = new SampleTable(row.getInt("id"), row.getInt("age"),row.getString("firstname"), row.getString("lastname"));
        return ad;
    }

     */

    public List<String> getAllColumnsFromTable(CassandraTable table){
        String query = "SELECT * FROM " + table.getTableName();
        //Creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        //Creating Session object
        Session session = cluster.connect("sample_demo");
        //Getting the ResultSet
        ResultSet result = session.execute(query);
        List<String> columnNames =
                result.getColumnDefinitions().asList().stream()
                        .map(cl -> cl.getName())
                        .collect(Collectors.toList());
        return columnNames;
    }
    public ResultSet getAllRowsFromTable(CassandraTable table){
        String query = "SELECT * FROM " + table.getTableName();
        //Creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        //Creating Session object
        Session session = cluster.connect("sample_demo");
        //Getting the ResultSet
        ResultSet result = session.execute(query);
        return result;
    }

    /*public String convertToJson(SampleTable sampleTable){
        return new Gson().toJson(sampleTable);
    }

     */
    public void createfile(String name){
        try {
            File myObj = new File(name);
            if (myObj.createNewFile()) {
                System.out.println("File created: " + myObj.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
    public void writefile(String name, String input){
        try {
            FileWriter myWriter = new FileWriter(name);
            myWriter.write(input);
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}
