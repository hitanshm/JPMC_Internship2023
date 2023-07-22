package org.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraConnector {

    private Cluster cluster;

    private Session session;
    //Connecting Cassandra
    public void connect(String node, Integer port, String user, String password) {
        Cluster.Builder b = Cluster.builder().addContactPoint(node).withCredentials(user, password);
        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();

        session = cluster.connect();
    }

    public Session getSession() {

        return this.session;
    }

    public void close() {
        session.close();
        cluster.close();
    }
}
