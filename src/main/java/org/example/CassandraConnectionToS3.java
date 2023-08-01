package org.example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.datastax.driver.core.Row;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class CassandraConnectionToS3 {
    public void storeFileInS3(String bucketName, String keyName, String filePath){

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        System.out.println(bucketName + " is the bucket name");
        System.out.println(keyName + " is the key name");
        System.out.println(filePath);
        com.amazonaws.services.s3.model.PutObjectRequest request = new com.amazonaws.services.s3.model.PutObjectRequest(bucketName, keyName, new File(filePath));
        s3Client.putObject(request);
    }
    public void connectAndStoreDataToS3(String tableName, List<Row> allRowsData){
        //Store the data in a file in local computer
        //Only temporary but later, don't store in file; instead, directly transfer data in memory to S3
        String folderPath = "C:\\JPMC_Internship_2023\\";
        LocalComputer localObj = new LocalComputer();

        String date = ZonedDateTime.now( ZoneId.systemDefault() ).format( DateTimeFormatter.ofPattern( "uuuu_MM_dd" ) );

        String folderName = tableName + "/" + date + "/";
        String bucketName = "chetan-test-bucket-1";
        String filePath = folderPath + tableName + ".txt";
        localObj.writefile(filePath, allRowsData);


        S3Client client = S3Client.builder().build();

        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName).key(folderName).build();

        client.putObject(request, RequestBody.empty());

        S3Waiter waiter = client.waiter();
        HeadObjectRequest requestWait = HeadObjectRequest.builder()
                .bucket(bucketName).key(folderName).build();

        WaiterResponse<HeadObjectResponse> waiterResponse = waiter.waitUntilObjectExists(requestWait);

        waiterResponse.matched().response().ifPresent(System.out::println);

        System.out.println("Folder " + folderName + " is ready.");

        //Writing the file content into S3
        String parquetFilePath = "C:\\Users\\cheta\\IdeaProjects\\cassandra\\data_" + tableName + ".parquet";
        String parquetKeyName = folderName + "data.parquet";
        storeFileInS3(bucketName, parquetKeyName, parquetFilePath);
    }
}
