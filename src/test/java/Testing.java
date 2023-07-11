import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.example.*;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Testing {
    private Session session;

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
    private KeyspaceRepository schemaRepository;
    @Before
    public void connect() {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042, "hitansh", "hitansh");
        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository("library",session);
    }
    @Test
    public void whenInsertingATable_thenInsertedCorrectly() {

        schemaRepository.createTable("accountdetails", "accountid","int","name","text", "balance","int");

        ResultSet result = session.execute(
                "SELECT * FROM " + "library" + ".accountdetails;");

        List<String> columnNames =
                result.getColumnDefinitions().asList().stream()
                        .map(cl -> cl.getName())
                        .collect(Collectors.toList());
        
        assertEquals(columnNames.size(), 3);
        assertTrue(columnNames.contains("accountid"));
        assertTrue(columnNames.contains("name"));
        assertTrue(columnNames.contains("balance"));
        AccountDetails accountDetails = new AccountDetails(1,"ram",1212);
        schemaRepository.insertRow("accountdetails", accountDetails);
        AccountDetails testad = schemaRepository.selectRow("accountdetails",1);
        String jsonstring = schemaRepository.convertToJson(testad);
        schemaRepository.createfile("C:\\JPMC project\\test.txt");
        schemaRepository.writefile("C:\\JPMC project\\test.txt",jsonstring);


    }
}
