package org.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        String keyspaceName ="library";
        String cassandraUser="hitansh";
        String cassandraPassword="hitansh";
        String awsBucketName="mytestfromjava45543";
        Cassandra cassandra = new Cassandra(cassandraUser,cassandraPassword);
        ArrayList<String> tables;
        tables = cassandra.getTables(keyspaceName);
        for (int i = 0; i < tables.size(); i++) {
            Multithreading multithreading = new Multithreading(i,keyspaceName, tables.get(i),awsBucketName,cassandraUser,cassandraPassword);
            multithreading.start();
        }
        //System.out.println("Project Successful");
        //System.exit(0);
    }
}
