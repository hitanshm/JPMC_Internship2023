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
        String keyName = CreateS3Folder.folderName + "test.txt";
        String filePath = "C:\\JPMC_Internship_2023\\test.txt";
        String long_date = ZonedDateTime.now( ZoneId.systemDefault() ).format( DateTimeFormatter.ofPattern( "uuuu_MM_dd_HH_mm_ss" ) );
        String date_compressed = long_date.substring(0,10);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        //Date date = new Date();
        if (date_compressed.equals(CreateS3Folder.date)){


            PutObjectRequest request = new PutObjectRequest(bucketName, keyName, new File(filePath));
            s3Client.putObject(request);

            System.out.println("JSON file stored in Amazon S3 under the folder " + CreateS3Folder.date);
        } else{
            System.out.println("ERROR: The folder matching today's timestamp does not exist! Please run CreateS3Folder.java first.");
        }

    }
}