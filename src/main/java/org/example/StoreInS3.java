package org.example;

import com.datastax.driver.core.Row;

import java.util.List;

import static org.example.CassandraQueries.connectToCassandraDatabase;

public class StoreInS3 {
    public static String getTableDataFromCassandraAndStoreInS3(String tableName, String keyspaceName){
        List<String> collumnNames;
        List<Row> allRowsData;


        //Surrounding connection, getColumns, getRows, and storeS3 with try catch to avoid errors during multithreading
        try{
            connectToCassandraDatabase();
        } catch(Exception e){
            e.printStackTrace();
            try{
                connectToCassandraDatabase();
            } catch(Exception e2){
                System.out.println("Unable to connect to cassandra database");
                e2.printStackTrace();
                return "FAIL";
            }
        }

        //Get data from Cassandra table
        try {
            collumnNames = CassandraQueries.getAllColumnsFromTable(tableName, keyspaceName);
        } catch (Exception e) {

            System.out.println("Get all collumns from table failed first time");
            e.printStackTrace();
            try {
                collumnNames = CassandraQueries.getAllColumnsFromTable(tableName, keyspaceName);
            } catch (Exception e2) {
                System.out.println("Get all collumns from table failed second time");
                e2.printStackTrace();
                return "FAIL";
            }
        }

        try{
            allRowsData = CassandraQueries.getAllRowsFromTable(tableName, collumnNames, keyspaceName);
        }catch(Exception e) {
            e.printStackTrace();
            try{
                allRowsData = CassandraQueries.getAllRowsFromTable(tableName, collumnNames, keyspaceName);
            }catch(Exception e2){
                System.out.println("Unable to get all rows from cassandra table");
                e2.printStackTrace();
                return "FAIL";
            }
        }


        try{
            CassandraConnectionToS3.connectAndStoreDataToS3(tableName, allRowsData);
        }catch(Exception e){
            e.printStackTrace();
            try{
                CassandraConnectionToS3.connectAndStoreDataToS3(tableName, allRowsData);
            }catch(Exception e2){
                System.out.println("Unable to connect and store data to S3");
                e2.printStackTrace();
                return "FAIL";
            }
        }

        return "SUCCESS";
    }
}
