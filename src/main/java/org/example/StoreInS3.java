package org.example;

import com.datastax.driver.core.Row;

import java.util.List;

//import static org.example.CassandraQueries.connectToCassandraDatabase;

public class StoreInS3 {
    public String getTableDataFromCassandraAndStoreInS3(String tableName, String keyspaceName){

        List<String> collumnNames;
        List<Row> allRowsData;
        CassandraQueries cassandraQueryObject = new CassandraQueries();

        //Surrounding connection, getColumns, getRows, and storeS3 with try catch to avoid errors during multithreading
        try{
            cassandraQueryObject.connectToCassandraDatabase();
        } catch(Exception e){
            e.printStackTrace();
            try{
                cassandraQueryObject.connectToCassandraDatabase();
            } catch(Exception e2){
                System.out.println("Unable to connect to cassandra database");
                e2.printStackTrace();
                return "FAIL";
            }
        }

        //Get data from Cassandra table
        try {
            collumnNames = cassandraQueryObject.getAllColumnsFromTable(tableName, keyspaceName);
        } catch (Exception e) {

            System.out.println("Get all collumns from table failed first time");
            e.printStackTrace();
            try {
                collumnNames = cassandraQueryObject.getAllColumnsFromTable(tableName, keyspaceName);
            } catch (Exception e2) {
                System.out.println("Get all collumns from table failed second time");
                e2.printStackTrace();
                return "FAIL";
            }
        }

        try{
            allRowsData = cassandraQueryObject.getAllRowsFromTable(tableName, collumnNames, keyspaceName);
        }catch(Exception e) {
            e.printStackTrace();
            try{
                allRowsData = cassandraQueryObject.getAllRowsFromTable(tableName, collumnNames, keyspaceName);
            }catch(Exception e2){
                System.out.println("Unable to get all rows from cassandra table");
                e2.printStackTrace();
                return "FAIL";
            }
        }

        CassandraConnectionToS3 cassandraObj = new CassandraConnectionToS3();
        try{
            cassandraObj.connectAndStoreDataToS3(tableName, allRowsData);
        }catch(Exception e){
            e.printStackTrace();
            try{
                cassandraObj.connectAndStoreDataToS3(tableName, allRowsData);
            }catch(Exception e2){
                System.out.println("Unable to connect and store data to S3");
                e2.printStackTrace();
                return "FAIL";
            }
        }

        return "SUCCESS";
    }
}
