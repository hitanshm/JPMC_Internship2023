import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.stream.Collectors;

import org.example.AccountDetails;
import org.example.CassandraConnector;
import org.example.KeyspaceRepository;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class Testing {
    private Session session;
    private KeyspaceRepository schemaRepository;
    @Before
    public void connect() {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);
        this.session = client.getSession();
        String keyspaceName1 = "library";
        schemaRepository = new KeyspaceRepository(keyspaceName1,session);
        schemaRepository.createKeyspace(keyspaceName1, "SimpleStrategy", 1);
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
        AccountDetails accountDetails = new AccountDetails(1,"ram",12);
        schemaRepository.insertRow("accountdetails", accountDetails);
        AccountDetails accountDetails2 = new AccountDetails(2,"sam",13);
        schemaRepository.insertRow("accountdetails", accountDetails2);
        AccountDetails accountDetails3 = new AccountDetails(3,"joe",14);
        schemaRepository.insertRow("accountdetails", accountDetails3);
        schemaRepository.createfile("C:\\Coding\\test.txt");
        String jsonstring = "";
        for(int i=1; i<=3;i++){
            AccountDetails testad = schemaRepository.selectRow("accountdetails", i);
            jsonstring += schemaRepository.convertToJson(testad) + "\n";
            schemaRepository.writefile("C:\\Coding\\test.txt",jsonstring);
        }



    }
}