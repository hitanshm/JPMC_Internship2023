import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.example.*;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Testing {


    /*private KeyspaceRepository schemaRepository;

    @Before
    public void connect() {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);
        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository(session);
    }
    @Test

    public void whenCreatingAKeyspace_thenCreated() {
        String keyspaceName = "library";
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
    }


    private TableRepository schemaRepository;
    //private Session session;

    @Before
    public void connect() {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042, "hitansh", "hitansh");
        this.session = client.getSession();
        schemaRepository = new TableRepository(session);
    }
    @Test
    public void whenCreatingATable_thenCreatedCorrectly() {
        schemaRepository.createTable("TestTable","id","int","name","text");

        ResultSet result = session.execute(
                "SELECT * FROM " + "library" + ".TestTable;");

        List<String> columnNames =
                result.getColumnDefinitions().asList().stream()
                        .map(cl -> cl.getName())
                        .collect(Collectors.toList());

        assertEquals(columnNames.size(), 2);
        assertTrue(columnNames.contains("id"));
        assertTrue(columnNames.contains("name"));
    }
    private Insertvalue schemaRepository;
    @Before
    public void connect() {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042, "hitansh", "hitansh");
        this.session = client.getSession();
        Insertvalue v = new Insertvalue(session,"library","testtable","id","name","5","max");
        schemaRepository =v;
    }
    @Test
    public void whenInsertingATable_thenInsertedCorrectly() {

        schemaRepository.insertTable();

        ResultSet result = session.execute(
                "SELECT id FROM " + "library" + ".TestTable");
        List<String> columnNames =
                result.getColumnDefinitions().asList().stream()
                        .map(cl -> cl.getName())
                        .collect(Collectors.toList());
        List<Row> rows = result.all();
        assertEquals(rows.size(), 5);
        }
        */
    private Session session;
    private KeyspaceRepository schemaRepository;
    //Connecting with a local computer node and default port with username and password
    @Before
    public void connect() {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042, "hitansh", "hitansh");
        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository("library", session);
    }
    @Test
    public void testingCassandra() {

        schemaRepository.createTable("accountdetails", "accountid","int","name","text", "balance","int");

        ResultSet result = session.execute(
                "SELECT * FROM " + "library" + ".accountdetails;");

        //gets the column names of the table
        List<String> columnNames =
                result.getColumnDefinitions().asList().stream()
                        .map(cl -> cl.getName())
                        .collect(Collectors.toList());
        //checks if the table has the right number of columns and names
        assertEquals(columnNames.size(), 3);
        assertTrue(columnNames.contains("accountid"));
        assertTrue(columnNames.contains("name"));
        assertTrue(columnNames.contains("balance"));
        //create an object with column details and inserts it into the table
        AccountDetails accountDetails = new AccountDetails(1,"john",1246);
        schemaRepository.insertRow("accountdetails", accountDetails);
        AccountDetails accountDetails2 = new AccountDetails(2,"sam",13076);
        schemaRepository.insertRow("accountdetails", accountDetails2);
        AccountDetails accountDetails3 = new AccountDetails(3,"joe",14246);
        schemaRepository.insertRow("accountdetails", accountDetails3);
        //creates a file on my local computer
        schemaRepository.createfile("C:\\JPMC project\\accountdetails.txt");
        String jsonstring = "";
        /*for(int i=1; i<=3;i++){
            AccountDetails testad = schemaRepository.selectRow("accountdetails", i);
            jsonstring += schemaRepository.convertToJson(testad) + "\n";
            schemaRepository.writefile("C:\\JPMC project\\accountdetails.txt",jsonstring);
        }*/
        //List<AccountDetails> tableData = schemaRepository.getTable("accountdetails");
        //System.out.println(schemaRepository.convertToJson(tableData));
        CassandraTable table = new CassandraTable("accountdetails");
        //System.out.println(table.getColumnNames());
        //System.out.println(schemaRepository.getAllColumnsFromTable(table));
        //List<Row> rs =schemaRepository.getAllFromTable(table).all();
        //System.out.println(schemaRepository.convertToJson(rs));
        //System.out.println(table.getColumnNames());
//        System.out.println(schemaRepository.getAllColumnsFromTable(table));
//        System.out.println(schemaRepository.getAllFromTable(table).all());
        System.out.println(schemaRepository.RowsToJson(schemaRepository.getAllFromTable(table).all(),
               schemaRepository.getAllColumnsFromTable(table)));
        List<Map<String, Object>> test= schemaRepository.RowsToMList(schemaRepository.getAllFromTable(table).all(),schemaRepository.getAllColumnsFromTable(table));
        KeyspaceRepository.parquetWriter(test);
    }
    @Test
    public void testParquet() {
        List<Map<String, Object>> mapList = new ArrayList<>();
        Map<String, Object> mp = new HashMap<>();
        mp.put("A", 1);
        mp.put("B", "AB");
        Map<String, Object> mp2 = new HashMap<>();
        mp2.put("A", 2);
        mp2.put("B", "ABC");
        mapList.add(mp);
        mapList.add(mp2);
        KeyspaceRepository.parquetWriter(mapList);
    }
}

