package org.example;

import com.datastax.driver.core.Session;

import java.util.ArrayList;

public class MultithreadingCass extends Thread {
    private int threadNumber;
    private String tableName;
    public MultithreadingCass(int threadNum, String tableNm){
        threadNumber=threadNum;
        tableName=tableNm;
    }
    private Session session;
    private KeyspaceRepository schemaRepository;
    @Override
public void run(){

        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042, "hitansh", "hitansh");
        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository("library",session);
        ArrayList<String> cn = new ArrayList<String>();
        cn.add("accountid");
        cn.add("name");
        cn.add("balance");
        CassandraTable table = new CassandraTable(tableName,cn);;
        System.out.println(schemaRepository.getAllColumnsFromTable(table));
        System.out.println(schemaRepository.getAllFromTable(table).all());
        System.out.println("thread number "+ threadNumber);

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
