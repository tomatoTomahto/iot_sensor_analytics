/**
 * Created by samir on 2017-01-27.
 */
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;

public class KuduTest {
    private static final String KUDU_MASTER = "localhost";

    public static void main(String[] args) {
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
        String tableName = "sensor";

        try {
            // Create a list of columns - key and value
            List<ColumnSchema> columns = new ArrayList(3);
            columns.add(new ColumnSchema.ColumnSchemaBuilder("record_time", Type.INT64)
                    .key(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("well_name", Type.STRING)
                    .key(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("sensor_0", Type.DOUBLE)
                    .nullable(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("sensor_1", Type.DOUBLE)
                    .nullable(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("sensor_2", Type.DOUBLE)
                    .nullable(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("incomplete", Type.BOOL)
                    .build());
            List<String> hashKeys = new ArrayList<>();
            hashKeys.add("record_time");
            hashKeys.add("well_name");

            if (client.tableExists(tableName)) {
                client.deleteTable(tableName);
                System.out.println("Deleted table " + tableName);
            }

            // Create a table with the columns specified
            Schema schema = new Schema(columns);
            client.createTable(tableName, schema,
                    new CreateTableOptions()
                            //.setRangePartitionColumns(rangeKeys)
                            .addHashPartitions(hashKeys, 3)
                            .setNumReplicas(1));

            System.out.println("Created table " + tableName);

            // Insert records into the newly created table
//            KuduTable table = client.openTable(tableName);
//            KuduSession session = client.newSession();
//            for (int i = 10; i < 13; i++) {
//                Insert insert = table.newInsert();
//                PartialRow row = insert.getRow();
//                row.addInt(0, i);
//                row.addString(1, "well A");
//                row.addString(2, "sensor_" + i%3);
//                session.apply(insert);
//            }
//            for (int i = 20; i < 23; i++) {
//                Insert insert = table.newInsert();
//                PartialRow row = insert.getRow();
//                row.addInt(0, i);
//                row.addString(1, "well B");
//                row.addString(2, "sensor_" + i%3);
//                session.apply(insert);
//            }
//            session.flush();
//
//            System.out.println("Inserted records");
//
//            // Read the "value" column from the table
//            List<String> projectColumns = new ArrayList<>(1);
//            projectColumns.add("tag_id");
//            KuduScanner scanner = client.newScannerBuilder(table)
//                    .setProjectedColumnNames(projectColumns)
//                    .build();
//            while (scanner.hasMoreRows()) {
//                RowResultIterator results = scanner.nextRows();
//                while (results.hasNext()) {
//                    RowResult result = results.next();
//                    System.out.println(result.getInt(0));
//                }
//            }
        } catch (Exception e) {
            System.out.println("Error");
            e.printStackTrace();
        } finally {
            try {
                client.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}