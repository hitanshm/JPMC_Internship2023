package org.example;

import com.datastax.driver.core.Session;
import org.example.Testing;

public class MultithreadThing extends Thread{
    private int threadNumber;

    private Session session;
    private KeyspaceRepository schemaRepository;

    private String tableName;
    public MultithreadThing(int threadNum, String tableNm){
        threadNumber=threadNum;
        tableName=tableNm;
    }

    /*public MultithreadThing(int i) {
        this.threadNumber = i;
    }

     */


    @Override
    public void run(){

        System.out.println("Thread " + tableName + " is running.");


        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);
        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository(session);


        CassandraTable table = new CassandraTable(tableName);
        System.out.println(schemaRepository.getAllColumnsFromTable(table));
        System.out.println(schemaRepository.getAllRowsFromTable(table).all());
        System.out.println("thread number "+ threadNumber);






    }
}
