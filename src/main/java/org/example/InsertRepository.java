package org.example;

import com.datastax.driver.core.Session;

public class InsertRepository {

        private Session session;

        public InsertRepository(Session session) {
            this.session = session;
        }

        /**
         * Method used to create any keyspace - schema.
         *
         * @param replicationStrategy the replication strategy.
         * @param numberOfReplicas    the number of replicas.
         */
        public void insertTable(String TableName, String column1, String column2, String insertc1, String insertc2) {
            StringBuilder sb = new StringBuilder("insert into ").append("library.").append(TableName)
                    .append(" (").append(column1).append(",").append(column2).append(") values (")
                    .append(insertc1).append(",'").append(insertc2).append("') IF NOT EXISTS;");

            final String query = sb.toString();

            session.execute(query);
        }
}