package org.example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.io.File;
import java.security.Timestamp;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class JsonS3 {
    public static void main(String[] args) {
        String bucketName = "chetan-test-bucket-1";
        String keyName = "test";
        String filePath = "C:\\JPMC_Internship_2023\\test.txt";

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        //Date date = new Date();

        keyName = keyName +"_"+  ZonedDateTime.now( ZoneId.systemDefault() ).format( DateTimeFormatter.ofPattern( "uuuu_MM_dd_HH_mm_ss" ) ) + ".txt";

        PutObjectRequest request = new PutObjectRequest(bucketName, keyName, new File(filePath));
        s3Client.putObject(request);

        System.out.println("JSON file stored in Amazon S3!");
    }
}