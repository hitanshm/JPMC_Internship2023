package org.example;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;

public class MultithreadingCass extends Thread {
    private int threadNumber;
    private String tableName;
    public MultithreadingCass(int threadNum, String tableNm){
        threadNumber=threadNum;
        tableName=tableNm;
    }
    private Session session;
    private KeyspaceRepository schemaRepository;

    public List<Row> getTables() {
        ResultSet tables = session.execute("select table_name from system_schema.tables WHERE keyspace_name = '" + "library" + "'");
        return tables.all();
    }
    public ArrayList<String> tableList(){
        ArrayList<String> tn = new ArrayList<String>();
        tn.add("accountdetails");
        tn.add("testtable");
        return tn;
    }

    @Override
public void run(){

        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042, "hitansh", "hitansh");
        this.session = client.getSession();
        schemaRepository = new KeyspaceRepository("library",session);
        CassandraTable table = new CassandraTable(tableName);
        System.out.println(schemaRepository.getAllColumnsFromTable(table));
        System.out.println(schemaRepository.getAllFromTable(table).all());
        System.out.println("thread number "+ threadNumber);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
