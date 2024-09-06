package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class CassandraParallelReader {

    // Number of threads for parallel querying
    private static final int NUM_THREADS = 10;

    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))  // Change this to match your DSE cluster IP/port
                .withKeyspace("ts")
                .build()) {

            // Define ranges for parallel reads (split by timestamp or validfrom range)
            long[] ranges = generateRanges(1724348000000L, 1724349004000L, NUM_THREADS);

            // ExecutorService to manage threads
            ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);

            List<Future<?>> futures = new ArrayList<>();

            // Submit read tasks to ExecutorService
            for (int i = 0; i < ranges.length - 1; i++) {
                long startRange = ranges[i];
                long endRange = ranges[i + 1];
                futures.add(executorService.submit(() -> parallelQuery(session, startRange, endRange)));
            }

            // Wait for all threads to complete
            for (Future<?> future : futures) {
                future.get(); // Wait for each task to finish
            }

            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Function to execute parallelized query in the range
    private static void parallelQuery(CqlSession session, long startRange, long endRange) {
        System.out.printf("Thread %s is querying range %d to %d\n", Thread.currentThread().getName(), startRange, endRange);

        // Query to select data in the given range
        SimpleStatement statement = SimpleStatement.builder("SELECT tradeid, validfrom, timestamp, producttype, inventory, status "
                        + "FROM system_calypso "
                        + "WHERE validfrom >= ? AND validfrom < ? "
                        + "ALLOW FILTERING")
                .addPositionalValues(startRange, endRange)
                .setPageSize(500) // Adjust the page size for paginated results
                .build();

        ResultSet rs = session.execute(statement);

        // Iterate through rows and process the data
        for (Row row : rs) {
            System.out.printf("TradeID: %s, ValidFrom: %s, Timestamp: %s, ProductType: %s, Inventory: %s, Status: %s\n",
                    row.getString("tradeid"),
                    row.getInstant("validfrom"),
                    row.getInstant("timestamp"),
                    row.getString("producttype"),
                    row.getString("inventory"),
                    row.getString("status"));
        }
    }

    // Helper function to generate ranges for parallel reads
    private static long[] generateRanges(long startTimestamp, long endTimestamp, int numThreads) {
        long rangeSize = (endTimestamp - startTimestamp) / numThreads;
        long[] ranges = new long[numThreads + 1];
        for (int i = 0; i <= numThreads; i++) {
            ranges[i] = startTimestamp + i * rangeSize;
        }
        return ranges;
    }
}
