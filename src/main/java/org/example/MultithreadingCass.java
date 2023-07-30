package org.example;

import com.datastax.driver.core.*;

import java.io.IOException;
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
        System.out.println(schemaRepository.RowsToJson(schemaRepository.getAllFromTable(table,"library").all(),
                schemaRepository.getAllColumnsFromTable(table,"library")));
        String filePath ="sample"+threadNumber+".parquet";
        List<Map<String, Object>> testParquet= schemaRepository.RowsToMList(schemaRepository.getAllFromTable(table,"library").all(),schemaRepository.getAllColumnsFromTable(table,"library"));
        KeyspaceRepository.parquetWriter(testParquet,table,filePath);
        JsonS3 jsonS3= new JsonS3();
        jsonS3.createFolder(filePath);
        ReadS3 readS3 = new ReadS3();
        readS3.readFromS3();
        try {
            System.out.println(schemaRepository.parquetReader(filePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("thread number "+ threadNumber);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
