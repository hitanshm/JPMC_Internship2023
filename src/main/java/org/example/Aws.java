package org.example;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.File;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;


public class Aws {
    public static String date = ZonedDateTime.now( ZoneId.systemDefault() ).format( DateTimeFormatter.ofPattern( "uuuu_MM_dd" ) );
    public static String folderName = date + "/";
    //creates a folder based on the data and uploads a file in that folder
    public void createFolderAndUploadFile(String bucket, String fileName) {

        String bucketName = bucket;
        String keyName = folderName + fileName;
        String filePath = fileName;
        String long_date = ZonedDateTime.now( ZoneId.systemDefault() ).format( DateTimeFormatter.ofPattern( "uuuu_MM_dd_HH_mm_ss" ) );
        String date_compressed = long_date.substring(0,10);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

        PutObjectRequest request = new PutObjectRequest(bucketName, keyName, new File(filePath));
        s3Client.putObject(request);
        System.out.println("finished creating folder and uploading file");
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
    //reads file paths in a bucket
    public void readFromS3(String bucketName){
        String bucket_name = bucketName;
        System.out.format("Objects in S3 bucket %s:\n", bucket_name);
        //need to add specific region
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_2).build();
        ListObjectsV2Result result = s3.listObjectsV2(bucket_name);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary os : objects) {
            System.out.println("* " + os.getKey());
        }
    }
}