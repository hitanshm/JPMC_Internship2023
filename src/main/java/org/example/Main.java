package org.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;

import java.util.ArrayList;
import java.util.Iterator;

public class Main {
    public static void main(String[] args) {
        String keyspaceName ="library";
        ArrayList<String> tables = (ArrayList<String>) Cassandra.getTables("library");
        for (int i = 0; i < tables.size(); i++) {
            MultithreadingCass multitest = new MultithreadingCass(i,keyspaceName, tables.get(i));
            multitest.start();
        }
        System.out.println("Project Successful");
        //System.exit(0);
    }
}
