package org.example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.datastax.driver.core.*;
import org.apache.cassandra.cql3.Json;
import org.example.CassandraConnector;
import org.example.KeyspaceRepository;
import org.example.SampleTable;
import org.junit.Before;
import org.junit.Test;

import org.example.JsonS3;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.size;
import static javafx.application.Platform.exit;
import static jdk.nashorn.internal.objects.ArrayBufferView.length;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Testing {
    private KeyspaceRepository schemaRepository;
    private Session session;
    private static int amtOfTables;

    public int getAmtOfTables(){
        return amtOfTables;
    }



    //public static List table_list = new ArrayList();

    public void connect() {
        //String hello = "Hello";
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);
        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository(session);
    }

    public void parallelProcessing(){
        int batchsize = 1;
        ExecutorService executorService = Executors.newFixedThreadPool(batchsize);
        List<Future<String>> resultFutures = new ArrayList<>();

        String bucketName = "chetan-test-bucket-1";
        String keyspaceName2 = "sample_demo";
        String keyName = CreateS3Folder.folderName + "test.txt";
        String filePath = "C:\\JPMC_Internship_2023\\test.txt";
        String long_date = ZonedDateTime.now( ZoneId.systemDefault() ).format( DateTimeFormatter.ofPattern( "uuuu_MM_dd_HH_mm_ss" ) );
        String date_compressed = long_date.substring(0,10);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        Testing obj = new Testing();
        System.out.println("Main has ran.");


        //Connects to Cassandra
        connect();
        //Creates keyspace if hasn't already
        whenCreatingAKeyspace_thenCreated();

        //Create list called tableArray and store table names inside list
        List<String> tableArray;
        tableArray = getTables(keyspaceName2);





        for (String tableName:tableArray){
            System.out.println("Processing next table: " + tableName);
            Callable<String> callableTask = () -> this.getTableDataFromCassandraAndStoreInS3(tableName, keyspaceName2);
            resultFutures.add(executorService.submit(callableTask));
        }


        List<String> responsesList = new ArrayList<>();
        for (Future<String> future:resultFutures){
            try {
                responsesList.add(future.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        executorService.shutdown();

        System.out.println("Did any fail?: " + responsesList.stream().anyMatch(response -> response.equals("FAILURE")));
    }



    public String getTableDataFromCassandraAndStoreInS3(String tableName, String keyspaceName){
        List<String> collumnNames;
        List<Row> allRowsData;

        //Get data from Cassandra table

        collumnNames = getAllColumnsFromTable(tableName, keyspaceName);
        allRowsData = getAllRowsFromTable(tableName, collumnNames, keyspaceName);
        //Convert data into Parquet format

        //Save the data into S3

        connectAndStoreDataToS3(tableName, allRowsData);
        return "SUCCESS";
    }

    public List<String> getAllColumnsFromTable(String tableName, String keyspaceName){
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
    public List<Row> getAllRowsFromTable(String tableName, List<String> collumnNames, String keyspaceName){
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

    private void whenCreatingAKeyspace_thenCreated() {
        String keyspaceName = "test3";
        schemaRepository.createKeyspace(keyspaceName, "SimpleStrategy", 1);

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



    public List getTables(String keyspaceName2){

        List table_names = new ArrayList();

        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Metadata metadata = cluster.getMetadata();
        Iterator<TableMetadata> tm = metadata.getKeyspace(keyspaceName2).getTables().iterator();

        while (tm.hasNext()){
            TableMetadata t = tm.next();
            table_names.add(t.getName());

        }
        amtOfTables = table_names.size();

        System.out.println("Table names: " + table_names);
        System.out.println("Total table count: " + amtOfTables);

        return table_names;



    }

    public void connectAndStoreDataToS3(String tableName, List<Row> allRowsData){
        //Store the data in a file in local computer
        //Only temporary but later, don't store in file; instead, directly transfer data in memory to S3
        String folderPath = "C:\\JPMC_Internship_2023\\";

        String date = ZonedDateTime.now( ZoneId.systemDefault() ).format( DateTimeFormatter.ofPattern( "uuuu_MM_dd" ) );

        String folderName = tableName + "/" + date + "/";
        String bucketName = "chetan-test-bucket-1";
        String keyName = folderName + "data.txt";
        String filePath = folderPath + tableName + ".txt";
        writefile(filePath, allRowsData);




        S3Client client = S3Client.builder().build();

        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName).key(folderName).build();

        client.putObject(request, RequestBody.empty());

        S3Waiter waiter = client.waiter();
        HeadObjectRequest requestWait = HeadObjectRequest.builder()
                .bucket(bucketName).key(folderName).build();

        WaiterResponse<HeadObjectResponse> waiterResponse = waiter.waitUntilObjectExists(requestWait);

        waiterResponse.matched().response().ifPresent(System.out::println);

        System.out.println("Folder " + folderName + " is ready.");

        //Writing the file content into S3

        storeFileInS3(bucketName, keyName, filePath);
    }

    public void writefile(String name, List<Row> input){
        try {
            FileWriter myWriter = new FileWriter(name);
            myWriter.write(input.toString());
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    public void storeFileInS3(String bucketName, String keyName, String filePath){

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        System.out.println(bucketName + " is the bucket name");
        System.out.println(keyName + " is the key name");
        System.out.println(filePath);
        com.amazonaws.services.s3.model.PutObjectRequest request = new com.amazonaws.services.s3.model.PutObjectRequest(bucketName, keyName, new File(filePath));
        s3Client.putObject(request);
    }


    public static void main(String[] args){
        Testing test = new Testing();
        test.parallelProcessing();
        //Before running this, make sure the folder for today has been created. If it has not, please run CreateS3Folder.java.

    }


    }








