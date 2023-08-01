package org.example;




import com.datastax.driver.core.*;

import org.apache.hadoop.conf.Configuration;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.json.simple.JSONObject;
import org.apache.avro.Schema;




import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;
import java.io.IOException;

import static com.amazonaws.services.cloudtraildata.AWSCloudTrailDataClient.builder;


public class ParquetRelated {


    //Unused, but can be experimented with
    //Reading parquet file from AWS
    public static void readParquetAWS() throws IOException {
        Path path = new Path("s3a://chetan-test-bucket-1/student/2023_07_31/data.parquet");
        Configuration conf = new Configuration();
        //conf.set(key)
        //conf.set(secret key)
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.setBoolean("fs.s3a.path.style.access", true);
        //Following line causes error, unable to resolve
        //conf.setBoolean(org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED, true);

        InputFile file = HadoopInputFile.fromPath(path, conf);
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file).build();
        GenericRecord record;
        while ((record = reader.read()) != null) {
            System.out.println(record);
        }
    }

    //Reading local parquet file
    public static String parquetReaderLocal(String filePath) throws IOException {
        List<SimpleGroup> simpleGroups = new ArrayList<>();
        ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        PageReadStore pages;
        while ((pages = reader.readNextRowGroup()) != null) {
            long rows = pages.getRowCount();
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

            for (int i = 0; i < rows; i++) {
                SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                simpleGroups.add(simpleGroup);
            }
        }
        reader.close();
        System.out.println("Finished reading parquet from " + filePath);
        return simpleGroups.toString();

    }

    public static List<Map<String, Object>> RowsToMList(List<Row> rows, List<String> columns) {
        List<Map<String, Object>> mList = new ArrayList<>();
        for (Row row : rows) {
            HashMap<String, Object> map = new HashMap<>();
            for (String column : columns) {
                map.put(column, row.getObject(column));
            }
            mList.add(map);
        }
        return mList;
    }



    public static void parquetWriter(List<Map<String, Object>> mList, String table, String fileName, String keyspaceName) {
        String tmpPath = fileName+".parquet";

        String schemaJson = "{\"namespace\": \"org.myorganization.mynamespace\"," //Not used in Parquet, can put anything
                + "\"type\": \"record\"," //Must be set as record
                + "\"name\": \"myrecordname\"," //Not used in Parquet, can put anything
                + "\"fields\": ["
                + " {\"name\": \"accountid\",  \"type\": \"string\"}"
                + ", {\"name\": \"balance\", \"type\": \"string\"}" //Required field
                + ", {\"name\": \"name\", \"type\": \"string\"}"
                + " ]}";
        List<String> columns = CassandraQueries.getAllColumnsFromTable(table, keyspaceName);
        String clms="";
        for(int i=0;i< columns.size()-1;i++) {
            clms+=" {\"name\": \""+columns.get(i)+"\",  \"type\": \"string\"},";
        }
        clms+=" {\"name\": \""+columns.get(columns.size()-1)+"\",  \"type\": \"string\"}";
        StringBuilder sb =new StringBuilder("{\"namespace\": \"org.myorganization.mynamespace\",")
                .append("\"type\": \"record\",").append("\"name\": \"myrecordname\",").append("\"fields\": [")
                .append(clms).append(" ]}");
        String parq=sb.toString();
        Schema.Parser parser = new Schema.Parser().setValidate(true);
        Schema schema=parser.parse(parq);

        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(new Path(tmpPath))
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withSchema(schema)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            List<GenericData.Record> recordList = new ArrayList<>();
            GenericData.Record record = new GenericData.Record(schema);
            mList.forEach((d) -> {
                try {
                    d.forEach((K, V) -> {
                        record.put(K, V.toString());
                    });
                    writer.write(record);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
