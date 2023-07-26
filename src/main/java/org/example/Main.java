package org.example;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.util.ArrayList;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] args) {
        ArrayList<String> tn = new ArrayList<String>();
        tn.add("accountdetails");
        tn.add("testtable");
        for (int i=0; i < tn.toArray().length; i++) {
            MultithreadingCass multitest = new MultithreadingCass(i,tn.get(i));
            multitest.start();
        }
    }
}