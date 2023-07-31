package org.example;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class Testing {
    //Run this file

    public void parallelProcessing(){
        int batchsize = 1;
        ExecutorService executorService = Executors.newFixedThreadPool(batchsize);
        List<Future<String>> resultFutures = new ArrayList<>();

        String keyspaceName2 = "sample_demo";

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

        System.out.println("Did any fail?: " + responsesList.stream().anyMatch(response -> response.equals("FAILURE")));

    }

    public static void main(String[] args) throws IOException {
        String filePath = "C:\\Users\\cheta\\IdeaProjects\\cassandra\\data_student.parquet";

        Testing test = new Testing();
        test.parallelProcessing();
        System.out.println(ParquetRelated.parquetReaderLocal(filePath));
        System.exit(0);

    }


}













