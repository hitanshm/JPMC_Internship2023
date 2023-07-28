package org.example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.io.File;

public class StoreJsonToS3 {
    public static void main(String[] args) {
        String bucketName = "mytestfromjava45543";
        String keyName = "accountdetails.txt";
        String filePath = "C:\\JPMC project\\accountdetails.txt";

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        PutObjectRequest request = new PutObjectRequest(bucketName, keyName, new File(filePath));
        s3Client.putObject(request);

        System.out.println("JSON file stored in Amazon S3!");
    }
}