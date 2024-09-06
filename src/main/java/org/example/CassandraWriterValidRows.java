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

public class CassandraWriterValidRows {

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

            for (int i = 0; i < 10000; i++) {
                // Generate values that will match the query condition
                Instant validFrom = Instant.ofEpochMilli(1724349004000L - new Random().nextInt(1000000));  // Random time before 1724349004000L
                Instant timestamp = Instant.ofEpochMilli(1724349004000L - new Random().nextInt(1000000));  // Random time before 1724349004000L
                Instant validTo = Instant.ofEpochMilli(1724349004000L + new Random().nextInt(1000000));    // Random time after 1724349004000L
                Instant knowledgeto = Instant.ofEpochMilli(1724349004000L + new Random().nextInt(1000000)); // Random time after 1724349004000L

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
                        randomInventory(),  // inventory (one of the values that matches your query)
                        randomBoolean(),  // iscleared
                        randomBoolean(),  // isinternal
                        knowledgeto,  // knowledgeto (must be > 1724349004000)
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
                        randomProductType(),  // producttype (one of 'FX_SWAP', 'FX_FORWARD', 'CASH')
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
                        randomStatus(),  // status (one of 'ROLLOVERED', 'FO_AMENDED', 'TERMINATED', 'FO_CONFIRMED', 'VERIFIED')
                        randomString(10),  // subtype
                        randomString(10),  // swaptype
                        randomString(10),  // system
                        randomString(10),  // topofhouse
                        randomString(10),  // tradecurrency
                        randomLocalDate(),  // tradedate
                        randomString(10),  // tradername
                        validTo,  // validto (must be > 1724349004000)
                        1,  // version
                        randomString(10),  // vicechair
                        randomString(10)  // volckerdesk
                ));
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

    // Inventory values that match the query condition
    private static String randomInventory() {
        String[] inventories = { "IRT_TLDIRT_FWD10", "FUNDING_IRT_TLDIRT", "IRT_TLDIRT_FWD7", "IRT_TLDIRT_FWD4TOTUS", "IRT_TLDIRT_FWD2",
                "IRT_TLDIRT_FWD5", "IRT_TLDIRT_FWD5TOTUS", "IRT_TLDIRT_FWD", "IRT_TLDIRT_FWD4", "IRT_TLDIRT_FWD3" };
        return inventories[new Random().nextInt(inventories.length)];
    }

    // Product types that match the query condition
    private static String randomProductType() {
        String[] productTypes = { "FX_SWAP", "FX_FORWARD", "CASH" };
        return productTypes[new Random().nextInt(productTypes.length)];
    }

    // Status values that match the query condition
    private static String randomStatus() {
        String[] statuses = { "ROLLOVERED", "FO_AMENDED", "TERMINATED", "FO_CONFIRMED", "VERIFIED" };
        return statuses[new Random().nextInt(statuses.length)];
    }
}
