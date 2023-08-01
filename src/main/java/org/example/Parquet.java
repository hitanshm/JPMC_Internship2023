package org.example;

import com.datastax.driver.core.Row;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Parquet {
    //converts data from Cassandra to MList
    public List<Map<String, Object>> RowsToMList(List<Row> rows, List<String> columns) {
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

    //writes and uploads a parquet file to the local computer of the Cassandra data in Mlist format
    public void parquetWriter(List<Map<String, Object>> mList, List<String> columns, String fileName) {
        String tmpPath = fileName;

        String clms = "";
        for (int i = 0; i < columns.size() - 1; i++) {
            clms += " {\"name\": \"" + columns.get(i) + "\",  \"type\": \"string\"},";
        }
        clms += " {\"name\": \"" + columns.get(columns.size() - 1) + "\",  \"type\": \"string\"}";
        StringBuilder sb = new StringBuilder("{\"namespace\": \"org.myorganization.mynamespace\",")
                .append("\"type\": \"record\",").append("\"name\": \"myrecordname\",").append("\"fields\": [")
                .append(clms).append(" ]}");
        String parq = sb.toString();
        Schema.Parser parser = new Schema.Parser().setValidate(true);
        Schema schema = parser.parse(parq);

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
        System.out.println("finished writing parquet file");
    }

    //reads parquet files stored on the local computer
    public String parquetReader(String filePath) throws IOException {
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
        return simpleGroups.toString();
    }
    public String getFileSize(String filepath){
        File file = new File(filepath);
        return filepath+ (double) file.length() / 1024 + " kb";
    }
}