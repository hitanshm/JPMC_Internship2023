package org.example;
import com.datastax.driver.core.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public class DataProcessor extends Thread{
    private String keyspaceName;
    private String tableName;
    private Session session;

    private String bucketName;
    private String cassandraUser;
    private String cassandraPassword;
    private String region;
    //change class name

    public DataProcessor(String keyspaceName, String tableName, String bucketName,String region, String cassandraUser, String cassandraPassword){
        this.keyspaceName=keyspaceName;
        this.tableName=tableName;
        this.bucketName=bucketName;
        this.region=region;
        this.cassandraUser=cassandraUser;
        this.cassandraPassword=cassandraPassword;
    }
    @Override
    public void run(){

        //Connecting to Cassandra
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042, cassandraUser, cassandraPassword);
        this.session = client.getSession();

        //initializing objects
        CassandraTable table = new CassandraTable(keyspaceName,tableName);
        Cassandra cassandra = new Cassandra(session, table, cassandraUser, cassandraPassword);
        Json json = new Json();
        Aws aws = new Aws();
        Parquet parquet = new Parquet();

        //stores all data from table
        List<Row> allData = cassandra.getAllFromTable(table);
        //stores all columns in that table
        List<String> columns = cassandra.getAllColumnsFromTable(table);

        //prints data retrieved from Cassandra in JSON format
        System.out.println(json.RowsToJson(allData,columns));

        //defines filepath of parquet file
        //it includes the table name so that different tables have separate files and it rewrites the file if called multiple times
        String filePath = tableName+"_data.parquet";
        //stores mapped data from Cassandra in an MList
        List<Map<String, Object>> mListData= parquet.RowsToMList(allData,columns);
        //creates a file on the local computer with the data from Cassandra in Parquet format
        parquet.parquetWriter(mListData,columns,filePath);

        //adds a folder with today's date and uploads the parquet file in AWS
        aws.createFolderAndUploadFile(bucketName,filePath);
        //reads files in AWS
        aws.readFromS3(bucketName,region);

        //reads parquet data from the local computer file (that was uploaded to AWS)
        try {
            System.out.println(parquet.parquetReader(filePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(parquet.getFileSize(filePath));
    }

}
