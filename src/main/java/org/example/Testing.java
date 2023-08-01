package org.example;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class Testing {
    //Run this file

    public void parallelProcessing(){
        ParquetRelated parquetObject = new ParquetRelated();
        CassandraRelated cassandraRelatedObj = new CassandraRelated();
        CassandraQueries cassandraQueryObj = new CassandraQueries();
        StoreInS3 s3StorageObj = new StoreInS3();
        int batchsize = 1;
        ExecutorService executorService = Executors.newFixedThreadPool(batchsize);
        List<Future<String>> resultFutures = new ArrayList<>();

        String keyspaceName2 = "sample_demo";

        //Testing obj = new Testing();
        System.out.println("Main has ran.");

        //Connects to Cassandra
        cassandraRelatedObj.connect();
        //Creates keyspace if hasn't already

        cassandraQueryObj.whenCreatingAKeyspace_thenCreated();

        //Create list called tableArray and store table names inside list
        List<String> tableArray;
        tableArray = cassandraRelatedObj.getTables(keyspaceName2);

        for (String tableName:tableArray){

            String fileName = "data_" + tableName;
            System.out.println("Processing next table: " + tableName);
            Callable<String> callableTask = () -> s3StorageObj.getTableDataFromCassandraAndStoreInS3(tableName, keyspaceName2);
            List<String> Collumns = cassandraQueryObj.getAllColumnsFromTable(tableName, keyspaceName2);
            List<Map<String, Object>> mList = parquetObject.RowsToMList(cassandraQueryObj.getAllRowsFromTable(tableName, Collumns, keyspaceName2), Collumns);
            parquetObject.parquetWriter(mList, tableName, fileName, keyspaceName2);
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

        System.out.println("Did any fail?: " + responsesList.stream().anyMatch(response -> response.equals("FAILURE")));

    }

    public static void main(String[] args) throws IOException {
        ParquetRelated parquetObject = new ParquetRelated();
        String filePath = "C:\\Users\\cheta\\IdeaProjects\\cassandra\\data_student.parquet";

        Testing test = new Testing();
        test.parallelProcessing();
        System.out.println(parquetObject.parquetReaderLocal(filePath));
        System.exit(0);

    }


}













