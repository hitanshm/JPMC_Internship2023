package org.example;

import com.datastax.driver.core.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MultithreadingCass extends Thread {
    private ArrayList<String> tableNames = new ArrayList<String>();

    private int threadNumber;
    private String keyspaceName;
    private String tableName;
    public MultithreadingCass(int threadNumber, String keyspaceName, String tableName){
        this.threadNumber=threadNumber;
        this.keyspaceName=keyspaceName;
        this.tableName=tableName;
    }
    private Session session;
    private KeyspaceRepository schemaRepository;

    @Override
public void run(){
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042, "hitansh", "hitansh");
        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository(keyspaceName,session);
        CassandraTable table = new CassandraTable(keyspaceName,tableName);
        List<Row> rows=schemaRepository.getAllFromTable(table,keyspaceName).all();
        List<String> columns=schemaRepository.getAllColumnsFromTable(table,keyspaceName);
        System.out.println(schemaRepository.RowsToJson(rows,columns));
        String filePath ="sample"+threadNumber+".parquet";
        List<Map<String, Object>> testParquet= schemaRepository.RowsToMList(rows,columns);
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
    }
}
