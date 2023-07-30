package org.example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.File;
import java.security.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import static org.example.CreateS3Folder.folderName;

public class JsonS3 {
    public static void createFolder() {

        String bucketName = "mytestfromjava45543";
        String keyName = folderName + "sample1.parquet";
        String filePath = "sample1.parquet";
        String long_date = ZonedDateTime.now( ZoneId.systemDefault() ).format( DateTimeFormatter.ofPattern( "uuuu_MM_dd_HH_mm_ss" ) );
        String date_compressed = long_date.substring(0,10);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        PutObjectRequest request = new PutObjectRequest(bucketName, keyName, new File(filePath));
        s3Client.putObject(request);
        //Date date = new Date();
        /*
        S3Waiter waiter = client.waiter();
        HeadObjectRequest requestWait = HeadObjectRequest.builder()
                .bucket(bucketName).key(folderName).build();

        WaiterResponse<HeadObjectResponse> waiterResponse = waiter.waitUntilObjectExists(requestWait);

        waiterResponse.matched().response().ifPresent(System.out::println);

        System.out.println("Folder " + folderName + " is ready.");

        //Writing the file content into S3
        String parquetFilePath = "sample1.parquet";
        String parquetKeyName = folderName + "sample1.parquet";
*/
    }

    static void putObjectIntoS3(String date_compressed, String bucketName, String keyName, String filePath, AmazonS3 s3Client){
        if (date_compressed.equals(CreateS3Folder.date)){


            PutObjectRequest request = new PutObjectRequest(bucketName, keyName, new File(filePath));
            s3Client.putObject(request);

            System.out.println("JSON file stored in Amazon S3 under the folder " + CreateS3Folder.date);
        } else{
            System.out.println("ERROR: The folder matching today's timestamp does not exist! Please run CreateS3Folder.java first.");
        }
    }
}