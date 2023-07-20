package org.example;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class CreateS3Folder {
    public static String date = ZonedDateTime.now( ZoneId.systemDefault() ).format( DateTimeFormatter.ofPattern( "uuuu_MM_dd" ) );
    public static String folderName = date + "/";
    public static void main(String[] args) {
        String bucketName = "chetan-test-bucket-1";

        //String folderName = date + "/";

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
    }
}