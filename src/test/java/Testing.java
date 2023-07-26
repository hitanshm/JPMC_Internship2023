import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.example.*;
import org.json.simple.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Testing {

    private Session session;
    private KeyspaceRepository schemaRepository;
    //Connecting with a local computer node and default port with username and password
    @Before
    public void connect() {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);
        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository("library",session);
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
        DataStorage accountDetails = new DataStorage(1,"john",1246);
        schemaRepository.insertRow("accountdetails", accountDetails);
        DataStorage accountDetails2 = new DataStorage(2,"sam",13076);
        schemaRepository.insertRow("accountdetails", accountDetails2);
        DataStorage accountDetails3 = new DataStorage(3,"joe",14246);
        schemaRepository.insertRow("accountdetails", accountDetails3);
        //creates a file on my local computer
        schemaRepository.createfile("C:\\JPMC project\\accountdetails.txt");
        String jsonstring = "";
        schemaRepository.writefile("C:\\JPMC\\CassandraTest.txt",jsonstring);
        ArrayList<String> cn = new ArrayList<String>();
        cn.add("accountid");
        cn.add("name");
        cn.add("balance");
        CassandraTable table = new CassandraTable("accountdetails",cn);
        //System.out.println(table.getColumnNames());
        System.out.println(schemaRepository.getAllColumnsFromTable(table));
        System.out.println(schemaRepository.getAllFromTable(table).all());
    }
}