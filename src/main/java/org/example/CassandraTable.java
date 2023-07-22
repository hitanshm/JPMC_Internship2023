package org.example;

import java.util.ArrayList;

public class CassandraTable {
    private String tabelName;
    private ArrayList<String> columnNames = new ArrayList<String>();
    public CassandraTable(String tn, ArrayList<String> cn){
        tabelName=tn;
        columnNames=cn;
    }
}
