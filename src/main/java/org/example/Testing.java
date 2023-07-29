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




/*
    //Cassandra
    public void connect() {
        //String hello = "Hello";
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);
        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository(session);
    }

 */

    public void parallelProcessing(){
        int batchsize = 1;
        ExecutorService executorService = Executors.newFixedThreadPool(batchsize);
        List<Future<String>> resultFutures = new ArrayList<>();

        String bucketName = "chetan-test-bucket-1";
        String keyspaceName2 = "sample_demo";
        String keyName = CreateS3Folder.folderName + "test.txt";

        String long_date = ZonedDateTime.now( ZoneId.systemDefault() ).format( DateTimeFormatter.ofPattern( "uuuu_MM_dd_HH_mm_ss" ) );
        String date_compressed = long_date.substring(0,10);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        //Testing obj = new Testing();
        System.out.println("Main has ran.");


        //Connects to Cassandra
        CassandraRelated.connect();
        //Creates keyspace if hasn't already

        CassandraQueries.whenCreatingAKeyspace_thenCreated();

        //Create list called tableArray and store table names inside list
        List<String> tableArray;
        tableArray = CassandraRelated.getTables(keyspaceName2);





        for (String tableName:tableArray){

            String fileName = "data_" + tableName;
            System.out.println("Processing next table: " + tableName);
            Callable<String> callableTask = () -> StoreInS3.getTableDataFromCassandraAndStoreInS3(tableName, keyspaceName2);
            List<String> Collumns = CassandraQueries.getAllColumnsFromTable(tableName, keyspaceName2);
            List<Map<String, Object>> mList = ParquetRelated.RowsToMList(CassandraQueries.getAllRowsFromTable(tableName, Collumns, keyspaceName2), Collumns);
            ParquetRelated.parquetWriter(mList, tableName, fileName, keyspaceName2);
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



        //executorService.shutdown();

        System.out.println("Did any fail?: " + responsesList.stream().anyMatch(response -> response.equals("FAILURE")));


    }

    public static void main(String[] args){

        Testing test = new Testing();
        test.parallelProcessing();
        System.exit(0);

    }


}













