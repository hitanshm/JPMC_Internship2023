import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.locator.SimpleStrategy;
import org.example.*;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CassandraTest {
    private Session session;
    private KeyspaceRepository schemaRepository;
    @Before
    public void connect() {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042); this.session = client.getSession();
        schemaRepository = new KeyspaceRepository("library",session);
    }
    @Test
    public void whenInsertingATable_thenInsertedCorrectly() {
        schemaRepository.createKeyspace("library", "SimpleStrategy", 1);

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
        schemaRepository.createfile("C:\\JPMC\\CassandraTest.txt");
        schemaRepository.writefile("C:\\JPMC\\CassandraTest.txt",jsonstring);



    }
}

