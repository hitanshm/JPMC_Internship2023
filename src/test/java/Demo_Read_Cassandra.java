import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class Demo_Read_Cassandra {

    public static void main(String args[])throws Exception{

        //queries
        String query = "SELECT * FROM emp";

        //Creating Cluster object
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

        //Creating Session object
        Session session = cluster.connect("sample_demo");

        //Getting the ResultSet
        ResultSet result = session.execute(query);

        System.out.println(result.all());
    }
}