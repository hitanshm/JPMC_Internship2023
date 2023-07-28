package org.example;

import com.datastax.driver.core.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MultithreadingCass extends Thread {
    private ArrayList<String> table_names = new ArrayList<String>();

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
        CassandraTable table = new CassandraTable(tableName);
        System.out.println(schemaRepository.RowsToJson(schemaRepository.getAllFromTable(table).all(),
                schemaRepository.getAllColumnsFromTable(table)));
        List<Map<String, Object>> testParquet= schemaRepository.RowsToMList(schemaRepository.getAllFromTable(table).all(),schemaRepository.getAllColumnsFromTable(table));
        KeyspaceRepository.parquetWriter(testParquet,table);
        System.out.println("thread number "+ threadNumber);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
