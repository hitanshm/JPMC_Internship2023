package org.example;

import com.datastax.driver.core.*;

import java.util.ArrayList;
import java.util.Iterator;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class TestRun {
    public static void main(String[] args) {
        String keyspaceName= "library";
        ArrayList<String> table_names = new ArrayList<String>();
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Metadata metadata = cluster.getMetadata();
        Iterator<TableMetadata> tm = metadata.getKeyspace(keyspaceName).getTables().iterator();

        while (tm.hasNext()) {
            TableMetadata t = tm.next();
            table_names.add(t.getName());
        }
        for (int i = 0; i < table_names.size(); i++) {
            MultithreadingTest multitest = new MultithreadingTest(i,keyspaceName, table_names.get(i));
            multitest.start();
        }


    }
}