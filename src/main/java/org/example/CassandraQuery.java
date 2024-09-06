package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.net.InetSocketAddress;
import java.time.Instant;

public class CassandraQuery {

    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("54.226.172.72", 9042)) // Replace with your DSE cluster IP/port
                .withKeyspace("ts") // Replace with your keyspace name
                .withLocalDatacenter("Cassandra") // Replace with your datacenter name
                .build()) {

            // Define the query
            String query = "SELECT tradeid, validfrom, timestamp, producttype, inventory, status FROM ts.system_calypso "
                    + "WHERE inventory IN ( 'IRT_TLDIRT_FWD10','FUNDING_IRT_TLDIRT','IRT_TLDIRT_FWD7','IRT_TLDIRT_FWD4TOTUS','IRT_TLDIRT_FWD2',"
                    + "'IRT_TLDIRT_FWD5','IRT_TLDIRT_FWD5TOTUS','IRT_TLDIRT_FWD','IRT_TLDIRT_FWD4','IRT_TLDIRT_FWD3' ) "
                    + "AND producttype IN ( 'FX_SWAP','FX_FORWARD','CASH' ) "
                    + "AND status IN ( 'ROLLOVERED','FO_AMENDED','TERMINATED','FO_CONFIRMED','VERIFIED' ) "
                    + "AND validfrom <= ? AND validto > ? "
                    + "AND timestamp <= ? AND knowledgeto > ? "
                    + "ALLOW FILTERING";

            // Define the parameters for the query
            Instant queryTime = Instant.ofEpochMilli(1724349004000L); // Use the same timestamp for the conditions
            SimpleStatement statement = SimpleStatement.builder(query)
                    .addPositionalValue(queryTime) // validfrom <= 1724349004000
                    .addPositionalValue(queryTime) // validto > 1724349004000
                    .addPositionalValue(queryTime) // timestamp <= 1724349004000
                    .addPositionalValue(queryTime) // knowledgeto > 1724349004000
                    .build();

            // Execute the query and get the result set
            ResultSet resultSet = session.execute(statement);

            // Iterate through the results and print them
            for (Row row : resultSet) {
                String tradeId = row.getString("tradeid");
                Instant validFrom = row.getInstant("validfrom");
                Instant timestamp = row.getInstant("timestamp");
                String productType = row.getString("producttype");
                String inventory = row.getString("inventory");
                String status = row.getString("status");

                System.out.printf("TradeID: %s, ValidFrom: %s, Timestamp: %s, ProductType: %s, Inventory: %s, Status: %s\n",
                        tradeId, validFrom, timestamp, productType, inventory, status);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
