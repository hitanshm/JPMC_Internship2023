package org.example;

import com.datastax.driver.core.Session;

public class TableRepository {
    private Session session;

    public TableRepository(Session session) {
        this.session = session;
    }

    /**
     * Method used to create any keyspace - schema.
     *
     * @param replicationStrategy the replication strategy.
     * @param numberOfReplicas    the number of replicas.
     */
    public void createTable(String TableName, String column1, String type1, String column2, String type2) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append("library.").append(TableName).append(" (").append(column1).append(" ").append(type1)
                .append(" primary key").append(",").append(column2).append(" ")
                .append(type2).append(");");

        final String query = sb.toString();

        session.execute(query);

    }
}
