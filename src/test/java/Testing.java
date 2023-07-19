import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.stream.Collectors;

import org.example.DataStorage;
import org.example.CassandraConnector;
import org.example.DataStorage;
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
        DataStorage accountDetails = new DataStorage(1,"ram",100);
        schemaRepository.insertRow("accountdetails", accountDetails);
        DataStorage accountDetails2 = new DataStorage(2,"bam",200);
        schemaRepository.insertRow("accountdetails", accountDetails2);
        DataStorage accountDetails3 = new DataStorage(3,"sam",300);
        schemaRepository.insertRow("accountdetails", accountDetails3);
        schemaRepository.createfile("C:\\JPMC\\CassandraTest.txt");
        String jsonstring = "";
        for(int i=1; i<=3;i++){
            DataStorage testad = schemaRepository.selectRow("accountdetails", i);
            jsonstring += schemaRepository.convertToJson(testad) + "\n";
            schemaRepository.writefile("C:\\JPMC\\CassandraTest.txt",jsonstring);
        }
    }
}