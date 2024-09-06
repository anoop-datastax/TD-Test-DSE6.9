package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.LocalDate;  // Replace java.sql.Date with java.time.LocalDate
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.Arrays;
import java.nio.ByteBuffer;

public class CassandraWriter {

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

            for (int i = 0; i < 50000; i++) {
                // Insert LocalDate instead of java.sql.Date
                session.execute(prepared.bind(
                        UUID.randomUUID().toString(),  // tradeid
                        Instant.ofEpochMilli(System.currentTimeMillis() - new Random().nextInt(1000000000)), // validfrom (Instant)
                        Instant.now(),  // timestamp (Instant)
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
                        randomInventory(),  // inventory
                        randomBoolean(),  // iscleared
                        randomBoolean(),  // isinternal
                        Instant.ofEpochMilli(System.currentTimeMillis() + 1000000000),  // knowledgeto (Instant)
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
                        randomProductType(),  // producttype
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
                        randomStatus(),  // status
                        randomString(10),  // subtype
                        randomString(10),  // swaptype
                        randomString(10),  // system
                        randomString(10),  // topofhouse
                        randomString(10),  // tradecurrency
                        randomLocalDate(),  // tradedate
                        randomString(10),  // tradername
                        Instant.ofEpochMilli(System.currentTimeMillis() + 1000000000),  // validto (Instant)
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

    // Update this method to return LocalDate instead of java.sql.Date
    private static LocalDate randomLocalDate() {
        long daysAgo = new Random().nextInt(365);  // Random date within the last year
        return LocalDate.now().minusDays(daysAgo);
    }

    // Update this method to return a List of LocalDate instead of java.sql.Date
    private static List<LocalDate> randomLocalDateList() {
        return Arrays.asList(randomLocalDate(), randomLocalDate(), randomLocalDate());
    }

    private static double randomDouble() {
        return 1000.0 + (5000.0 * new Random().nextDouble());
    }

    private static String randomInventory() {
        String[] inventories = { "IRT_TLDIRT_FWD10", "FUNDING_IRT_TLDIRT", "IRT_TLDIRT_FWD7", "IRT_TLDIRT_FWD4TOTUS", "IRT_TLDIRT_FWD2",
                "IRT_TLDIRT_FWD5", "IRT_TLDIRT_FWD5TOTUS", "IRT_TLDIRT_FWD", "IRT_TLDIRT_FWD4", "IRT_TLDIRT_FWD3" };
        return inventories[new Random().nextInt(inventories.length)];
    }

    private static String randomProductType() {
        String[] productTypes = { "FX_SWAP", "FX_FORWARD", "CASH" };
        return productTypes[new Random().nextInt(productTypes.length)];
    }

    private static String randomStatus() {
        String[] statuses = { "ROLLOVERED", "FO_AMENDED", "TERMINATED", "FO_CONFIRMED", "VERIFIED" };
        return statuses[new Random().nextInt(statuses.length)];
    }
}
