package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.Arrays;
import java.nio.ByteBuffer;

public class CassandraWriterTwentyMillion {

    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("54.226.172.72", 9042)) // Replace with your DSE cluster IP/port
                .withKeyspace("ts") // Replace with your keyspace name
                .withLocalDatacenter("Cassandra") // Replace with your datacenter name
                .build()) {

            String insertQuery = "INSERT INTO system_calypso (tradeid, validfrom, timestamp, archived, bookname, business, counterpartycode, "
                    + "counterpartyid, counterpartyname, counterpartyshortcode, description, desk, enddate, entereddate, event, extendedtype, "
                    + "externalreference, futuretype, globalbusiness, hasfees, idx, includedinoismmb, internalreference, inventory, iscleared, "
                    + "isinternal, knowledgeto, legalentity, manualfxreset, maturitydate, mktdatakey, mostrecentterminationfeedate, payindex, "
                    + "paylegcurrency, payload, paynotional, portfolio, producttype, recindex, reclegcurrency, recnotional, refdatakey, "
                    + "reportableeventtype, reportableproducttype, resetdate, segment, settlecurrency, settledate, srcid, status, subtype, "
                    + "swaptype, system, topofhouse, tradecurrency, tradedate, tradername, validto, version, vicechair, volckerdesk) "
                    + "VALUES (?,?,?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement prepared = session.prepare(insertQuery);

            long totalRows = 20_000_000;  // 20 million rows
            long halfRows = totalRows / 2;  // 50% rows for the specific condition

            for (long i = 0; i < totalRows; i++) {
                // 50% rows meet the query condition
                boolean meetsCondition = i < halfRows;

                // Generate values that will match the query condition
                Instant validFrom = meetsCondition ? Instant.ofEpochMilli(1724349004000L - new Random().nextInt(1000000)) : randomTimestamp();
                Instant timestamp = meetsCondition ? Instant.ofEpochMilli(1724349004000L - new Random().nextInt(1000000)) : randomTimestamp();
                Instant validTo = meetsCondition ? Instant.ofEpochMilli(1724349004000L + new Random().nextInt(1000000)) : randomTimestamp();
                Instant knowledgeto = meetsCondition ? Instant.ofEpochMilli(1724349004000L + new Random().nextInt(1000000)) : randomTimestamp();

                session.execute(prepared.bind(
                        UUID.randomUUID().toString(),  // tradeid
                        validFrom,  // validfrom
                        timestamp,  // timestamp
                        false,  // archived
                        randomString(10),  // bookname
                        randomString(10),  // business
                        randomString(10),  // counterpartycode
                        new Random().nextInt(1000),  // counterpartyid
                        randomString(10),  // counterpartyname
                        randomString(5),  // counterpartyshortcode
                        randomString(20),  // description
                        randomString(10),  // desk
                        randomLocalDate(),  // enddate
                        randomLocalDate(),  // entereddate
                        randomString(10),  // event
                        randomString(10),  // extendedtype
                        randomString(15),  // externalreference
                        randomString(10),  // futuretype
                        randomString(10),  // globalbusiness
                        randomBoolean(),  // hasfees
                        randomString(10),  // idx
                        randomBoolean(),  // includedinoismmb
                        randomString(10),  // internalreference
                        randomInventory(),  // inventory (based on cardinality)
                        randomBoolean(),  // iscleared
                        randomBoolean(),  // isinternal
                        knowledgeto,  // knowledgeto
                        randomString(10),  // legalentity
                        randomBoolean(),  // manualfxreset
                        randomLocalDate(),  // maturitydate
                        randomString(10),  // mktdatakey
                        randomLocalDate(),  // mostrecentterminationfeedate
                        randomString(10),  // payindex
                        randomString(10),  // paylegcurrency
                        null,  // payload (if needed)
                        randomDouble(),  // paynotional
                        randomString(10),  // portfolio
                        randomProductType(),  // producttype (based on cardinality)
                        randomString(10),  // recindex
                        randomString(10),  // reclegcurrency
                        randomDouble(),  // recnotional
                        randomString(10),  // refdatakey
                        randomString(10),  // reportableeventtype
                        randomString(10),  // reportableproducttype
                        randomLocalDateList(),  // resetdate (list<LocalDate>)
                        randomString(10),  // segment
                        randomString(10),  // settlecurrency
                        randomLocalDate(),  // settledate
                        randomString(10),  // srcid
                        randomStatus(),  // status (based on cardinality)
                        randomString(10),  // subtype
                        randomString(10),  // swaptype
                        randomString(10),  // system
                        randomString(10),  // topofhouse
                        randomString(10),  // tradecurrency
                        randomLocalDate(),  // tradedate
                        randomString(10),  // tradername
                        validTo,  // validto
                        1,  // version
                        randomString(10),  // vicechair
                        randomString(10)  // volckerdesk
                ));

                // Print progress every 100k rows
                if (i % 100_000 == 0) {
                    System.out.printf("Inserted %d rows...\n", i);
                }
            }

            System.out.println("Data inserted successfully!");
        }
    }

    // Helper functions for generating random values
    private static String randomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder(length);
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        return sb.toString();
    }

    private static boolean randomBoolean() {
        return new Random().nextBoolean();
    }

    // Generate LocalDate
    private static LocalDate randomLocalDate() {
        long daysAgo = new Random().nextInt(365);  // Random date within the last year
        return LocalDate.now().minusDays(daysAgo);
    }

    // Generate List<LocalDate>
    private static List<LocalDate> randomLocalDateList() {
        return Arrays.asList(randomLocalDate(), randomLocalDate(), randomLocalDate());
    }

    private static double randomDouble() {
        return 1000.0 + (5000.0 * new Random().nextDouble());
    }

    // Generate random timestamps
    private static Instant randomTimestamp() {
        long currentMillis = System.currentTimeMillis();
        long randomOffset = new Random().nextInt(1000000000);  // Random offset within ~11 days
        return Instant.ofEpochMilli(currentMillis - randomOffset);
    }

    // Inventory values based on the given cardinality
    private static String randomInventory() {
        String[] inventories = { "IRT_TLDIRT_FWD4", "IRT_TLDIRT_FWD3", "IRT_TLDIRT_FWD5", "IRT_TLDIRT_FWD", "IRT_TLDIRT_FWD5TOTUS" };
        int[] cardinalities = { 132063, 90058, 59389, 28709, 19941 };
        return weightedRandomSelection(inventories, cardinalities);
    }

    // Product types based on the given cardinality
    private static String randomProductType() {
        String[] productTypes = { "FX_SWAP", "CASH", "FX_FORWARD" };
        int[] cardinalities = { 1055920, 1764568, 208561 };
        return weightedRandomSelection(productTypes, cardinalities);
    }

    // Status values based on the given cardinality
    private static String randomStatus() {
        String[] statuses = { "VERIFIED", "TERMINATED", "FO_CONFIRMED", "FO_AMENDED", "ROLLOVERED" };
        int[] cardinalities = { 16787806, 7837182, 587015, 186583, 8411 };
        return weightedRandomSelection(statuses, cardinalities);
    }

    // Helper method to select a value based on weighted probabilities
    private static String weightedRandomSelection(String[] values, int[] cardinalities) {
        int total = Arrays.stream(cardinalities).sum();
        int random = new Random().nextInt(total);

        for (int i = 0; i < values.length; i++) {
            if (random < cardinalities[i]) {
                return values[i];
            }
            random -= cardinalities[i];
        }

        return values[0];  // Fallback (shouldn't happen)
    }
}
