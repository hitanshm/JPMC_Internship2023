package org.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CassandraRelated {
    public KeyspaceRepository schemaRepository;
    public Session session;
    public int amtOfTables;
    public List getTables(String keyspaceName2){

        List table_names = new ArrayList();

        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Metadata metadata = cluster.getMetadata();
        Iterator<TableMetadata> tm = metadata.getKeyspace(keyspaceName2).getTables().iterator();

        while (tm.hasNext()){
            TableMetadata t = tm.next();
            table_names.add(t.getName());

        }
        amtOfTables = table_names.size();

        System.out.println("Table names: " + table_names);
        System.out.println("Total table count: " + amtOfTables);

        return table_names;

    }

    public void connect() {
        //String hello = "Hello";
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);
        session = client.getSession();
        schemaRepository = new KeyspaceRepository(session);
    }
}
