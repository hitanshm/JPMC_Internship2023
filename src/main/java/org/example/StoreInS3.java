package org.example;

import com.datastax.driver.core.Row;

import java.util.List;

public class StoreInS3 {
    /*public static String getTableDataFromCassandraAndStoreInS3(String tableName, String keyspaceName){
        List<String> collumnNames;
        List<Row> allRowsData;

        //Get data from Cassandra table
        CassandraTable table = new CassandraTable(tableName);

        collumnNames = KeyspaceRepository.getAllColumnsFromTable(table, keyspaceName);
        allRowsData = KeyspaceRepository.getAllFromTable(table, "library");
        //Convert data into Parquet format

        //Save the data into S3

        CassandraConnectionToS3.connectAndStoreDataToS3(tableName, allRowsData);
        return "SUCCESS";
    }*/
}