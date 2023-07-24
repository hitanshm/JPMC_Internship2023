package org.example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.cql3.Json;
import org.example.CassandraConnector;
import org.example.KeyspaceRepository;
import org.example.SampleTable;
import org.junit.Before;
import org.junit.Test;

import org.example.JsonS3;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

import static javafx.application.Platform.exit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Testing {
    private KeyspaceRepository schemaRepository;
    private Session session;

    private void connect() {
        //String hello = "Hello";
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);
        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository(session);
    }


    private void whenCreatingAKeyspace_thenCreated() {
        String keyspaceName = "test3";
        schemaRepository.createKeyspace(keyspaceName, "SimpleStrategy", 1);

        ResultSet result =
                session.execute("SELECT * FROM system_schema.keyspaces;");

        List<String> matchedKeyspaces = result.all()
                .stream()
                .filter(r -> r.getString(0).equals(keyspaceName.toLowerCase()))
                .map(r -> r.getString(0))
                .collect(Collectors.toList());

        assertEquals(matchedKeyspaces.size(), 1);
        assertTrue(matchedKeyspaces.get(0).equals(keyspaceName.toLowerCase()));

        SampleTable testad = schemaRepository.selectRow("student");
        String jsonstring = schemaRepository.convertToJson(testad);
        schemaRepository.createfile("C:\\JPMC_Internship_2023\\test3.txt");
        schemaRepository.writefile("C:\\JPMC_Internship_2023\\test3.txt",jsonstring);


        System.out.println("Process has ran successfully.");

    }

    public static void main(String[] args){
        String bucketName = "chetan-test-bucket-1";
        String keyName = CreateS3Folder.folderName + "test.txt";
        String filePath = "C:\\JPMC_Internship_2023\\test.txt";
        String long_date = ZonedDateTime.now( ZoneId.systemDefault() ).format( DateTimeFormatter.ofPattern( "uuuu_MM_dd_HH_mm_ss" ) );
        String date_compressed = long_date.substring(0,10);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        Testing obj = new Testing();
        System.out.println("Main has ran.");

        obj.connect();

        obj.whenCreatingAKeyspace_thenCreated();

        JsonS3.logic(date_compressed, bucketName, keyName, filePath, s3Client);

        System.exit(0);
    }

    //@Before






}