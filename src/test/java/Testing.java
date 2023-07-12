import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.example.CassandraConnector;
import org.example.KeyspaceRepository;
import org.example.SampleTable;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Testing {
    private KeyspaceRepository schemaRepository;
    private Session session;

    @Before
    public void connect() {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);
        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository(session);
    }

    @Test
    public void whenCreatingAKeyspace_thenCreated() {
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
        schemaRepository.createfile("C:\\JPMC_Internship_2023\\test.txt");
        schemaRepository.writefile("C:\\JPMC_Internship_2023\\test.txt",jsonstring);


        System.out.println("Process has ran successfully.");

    }


}