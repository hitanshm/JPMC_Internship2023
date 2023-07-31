package org.example;




import com.datastax.driver.core.*;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.json.simple.JSONObject;
import org.apache.avro.Schema;




import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;
import java.io.IOException;


public class ParquetRelated {

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
