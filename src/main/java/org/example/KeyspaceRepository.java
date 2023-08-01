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
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.json.simple.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;

import org.apache.avro.Schema;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.FileSystem;
import java.util.*;
import java.util.stream.Collectors;

import static com.amazonaws.services.databasemigrationservice.model.DataFormatValue.Parquet;
import static com.sun.deploy.trace.Trace.createTempFile;
import static org.apache.parquet.example.Paper.schema;

public class KeyspaceRepository {
    private Session session;
    private String keyspace;

    public KeyspaceRepository(String keyspace, Session session) {

        this.session = session;
        this.keyspace = keyspace;
    }

    /**
     * Method used to create any keyspace - schema.
     *
     * @param keyspaceName        the name of the keyspaceName.
     * @param replicationStrategy the replication strategy.
     * @param numberOfReplicas    the number of replicas.
     */
    /*public void createKeyspace(String keyspaceName, String replicationStrategy, int numberOfReplicas) {
        StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                .append(keyspaceName).append(" WITH replication = {")
                .append("'class':'").append(replicationStrategy)
                .append("','replication_factor':").append(numberOfReplicas).append("};");

        final String query = sb.toString();

        session.execute(query);
    }*/

    public void createTable(String tableName, String column1, String type1, String column2, String type2, String column3, String type3) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(keyspace).append(".").append(tableName).append(" (").append(column1).append(" ").append(type1)
                .append(" primary key,").append(column2).append(" ")
                .append(type2).append(",").append(column3).append(" ")
                .append(type3).append(");");

        final String query = sb.toString();

        session.execute(query);

    }

    public void insertRow(String tableName, AccountDetails accountDetails) {
        StringBuilder sb = new StringBuilder("insert into ").append(keyspace).append(".").append(tableName)
                .append(" (").append("accountid").append(",").append("name").append(",").append("balance").append(") values (")
                .append(accountDetails.accountId).append(",'").append(accountDetails.name).append("',").append(accountDetails.balance).append(") IF NOT EXISTS;");

        final String query = sb.toString();

        session.execute(query);
    }
    /*
        public void selectRow(String tableName, int accountid) {
            StringBuilder sb = new StringBuilder("select * from ").append(keyspace).append(".")
                    .append(tableName).append(" WHERE accountid=").append(accountid).append(";");

            final String query = sb.toString();

            ResultSet result = session.execute(query);
            //make loop until row ends similar to result.next
            List<Row> row = result.all();
            row.isEmpty();

            //AccountDetails ad = new AccountDetails(row.get("accountId"), row.getString("name"), row.getInt("balance"));
            //return ad;
        }

        public List<AccountDetails> getTable(String tableName) {
            String query = "SELECT * FROM" + " accountdetails";

            //Creating Cluster object
            Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withCredentials("hitansh", "hitansh").build();

            //Creating Session object
            Session session = cluster.connect("library");

            //Getting the ResultSet
            ResultSet result = session.execute(query);

            List<AccountDetails> accountDetails = result.all()
                    .stream()
                    .map(row -> new AccountDetails(row.getInt("accountId"), row.getString("name"),row.getInt("balance")))
                    .collect(Collectors.toList());
            return accountDetails;
    }*/
    public static List<String> getAllColumnsFromTable(CassandraTable table, String keyspaceName){
        String query = "SELECT * FROM " + table.getTableName();
        //Creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withCredentials("hitansh", "hitansh").build();
        //Creating Session object
        Session session = cluster.connect(keyspaceName);
        //Getting the ResultSet
        ResultSet result = session.execute(query);
        List<String> columnNames =
                result.getColumnDefinitions().asList().stream()
                        .map(cl -> cl.getName())
                        .collect(Collectors.toList());
        return columnNames;
    }
public static ResultSet getAllFromTable(CassandraTable table, String keyspace){
        String query = "SELECT * FROM " + table.getTableName();
        //Creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withCredentials("hitansh", "hitansh").build();
        //Creating Session object
        Session session = cluster.connect(keyspace);
        //Getting the ResultSet
        ResultSet result = session.execute(query);
        return result;
    }

    /*public String convertToJson(List<Row> rs){
        return new Gson().toJson(rs);
    }*/
    public String RowsToJson(List<Row> rows, List<String> columns) {
        List<String> json = new ArrayList<>();
        for (Row row : rows) {
            HashMap<String, Object> map = new HashMap<>();
            for (String column : columns) {
                map.put(column, row.getObject(column));
            }
            Gson gson = new Gson();
            Type typeObject = new TypeToken<HashMap>() {}.getType();
            String gsonData = gson.toJson(map, typeObject);
            json.add(gsonData);
        }
        return json.toString();
    }
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
    public static void parquetWriter(List<Map<String, Object>> mList, CassandraTable table, String fileName) {
        String tmpPath = fileName;
        /*Schema schema = null;
        schema = new Schema.Parser().parse( "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"myrecord\",\n" +
                "  \"fields\": [ {\n" +
                "    \"name\": \"accountid\",\n" +
                "    \"type\": \"int\"\n" +
                "  } ]\n" +
                "}" ); */
        String schemaJson = "{\"namespace\": \"org.myorganization.mynamespace\"," //Not used in Parquet, can put anything
                + "\"type\": \"record\"," //Must be set as record
                + "\"name\": \"myrecordname\"," //Not used in Parquet, can put anything
                + "\"fields\": ["
                + " {\"name\": \"accountid\",  \"type\": \"string\"}"
                + ", {\"name\": \"balance\", \"type\": \"string\"}" //Required field
                + ", {\"name\": \"name\", \"type\": \"string\"}"
                + " ]}";
        List<String> columns =getAllColumnsFromTable(table,"library");
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
    public static String parquetReader(String filePath) throws IOException {
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
    /*public static Parquet getParquetData(String filePath) throws IOException {
        List<SimpleGroup> simpleGroups = new ArrayList<>();
        ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
        MessageType schema = reader.getFooter().getFileMetaData().getSchema();
        List<Type> fields = schema.getFields();
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
        return new Parquet(simpleGroups, fields);
    }

*/
    private static FileSystem createTempFile() {
        return null;
    }

    private ArrayList<String> table_names = new ArrayList<String>();
    public ArrayList<String> getTableNames(){
    Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withCredentials("hitansh","hitansh").build();
    Metadata metadata = cluster.getMetadata();
    Iterator<TableMetadata> tm = metadata.getKeyspace("library").getTables().iterator();
        while (tm.hasNext()){
        TableMetadata t = tm.next();
        table_names.add(t.getName());
    }
        return table_names;
    }

    public void createfile(String name){
        try {
            File myObj = new File(name);
            if (myObj.createNewFile()) {
                System.out.println("File created: " + myObj.getName());
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
    public void writefile(String name, String input){
        try {
            FileWriter myWriter = new FileWriter(name);
            myWriter.write(input);
            myWriter.close();
            System.out.println("Successfully wrote to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}
