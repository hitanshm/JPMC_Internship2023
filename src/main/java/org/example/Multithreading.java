package org.example;
import com.datastax.driver.core.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
public class Multithreading extends Thread{
    private ArrayList<String> tableNames = new ArrayList<String>();
    private int threadNumber;
    private String keyspaceName;
    private String tableName;
    private Session session;
    private Cassandra cassandra;
    private Json json;
    private AWS aws;
    private Parquet parquet;

    public Multithreading(int threadNumber, String keyspaceName, String tableName){
        this.threadNumber=threadNumber;
        this.keyspaceName=keyspaceName;
        this.tableName=tableName;
    }
    @Override
    public void run(){
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042, "hitansh", "hitansh");
        this.session = client.getSession();
        cassandra = new Cassandra(session);
        CassandraTable table = new CassandraTable(keyspaceName,tableName);
        List<Row> rows=cassandra.getAllFromTable(table,keyspaceName).all();
        List<String> columns=cassandra.getAllColumnsFromTable(table,keyspaceName);
        System.out.println(json.RowsToJson(rows,columns));
        String filePath ="sample"+threadNumber+".parquet";
        List<Map<String, Object>> testParquet= parquet.RowsToMList(rows,columns);
        parquet.parquetWriter(testParquet,table,filePath);
        aws.createFolder("mytestfromjava45543",filePath);
        aws.readFromS3("mytestfromjava45543");
        try {
            System.out.println(parquet.parquetReader(filePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
