package org.example;
import java.util.ArrayList;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Main main=new Main();
        main.batchProcessingThroughMultithreading();
        Thread.sleep(30000);
        System.out.println("Project Successful");
        System.exit(0);
    }
    public void batchProcessingThroughMultithreading(){
        String keyspaceName ="jpmc_data";
        String cassandraUser="hitansh";
        String cassandraPassword="hitansh";
        String awsBucketName="mytestfromjava45543";
        Cassandra cassandra = new Cassandra(cassandraUser,cassandraPassword);
        ArrayList<String> tables;
        tables = cassandra.getTables(keyspaceName);
        for (int i = 0; i < tables.size(); i++) {
            DataProcessor multithreading = new DataProcessor(i,keyspaceName, tables.get(i),awsBucketName,cassandraUser,cassandraPassword);
            multithreading.start();
        }

    }
}
