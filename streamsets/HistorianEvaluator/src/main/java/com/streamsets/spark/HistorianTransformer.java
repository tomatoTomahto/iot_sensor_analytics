package com.streamsets.spark;

// Streamsets Libraries and SDK
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.spark.api.SparkTransformer;
import com.streamsets.pipeline.spark.api.TransformResult;

// Spark SQL Libraries
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import static org.apache.spark.sql.functions.*;

// Spark Core Java Libraries
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

// Java Libraries
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import scala.Tuple2;

public class HistorianTransformer extends SparkTransformer implements Serializable {
    private transient JavaSparkContext javaSparkContext;
    private transient SQLContext sqlContext ;
    private transient DataFrame tagMappingsDF;
    private transient Map<String, String> arguments;

    // Sensor class to read in data from Streamsets
    public static class Sensor implements Serializable {
        private double value;
        private int tagID;
        private String recordTime;

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }

        public int getTagID() {
            return tagID;
        }

        public void setTagID(int tagID) {
            this.tagID = tagID;
        }

        public String getRecordTime() {
            return recordTime;
        }

        public void setRecordTime(String recordTime) {
            this.recordTime = recordTime;
        }
    }

    @Override
    public void init(JavaSparkContext javaSparkContext, List<String> params) {
        this.javaSparkContext = javaSparkContext;
        this.sqlContext = new SQLContext(javaSparkContext);
        this.arguments = new HashMap<>();

        // Parse out arguments passed to Spark from Streamsets
        for (String param : params) {
            String key = param.substring(0, param.indexOf('='));
            String value = param.substring(param.indexOf('=')+1, param.length());
            arguments.put(key, value);
        }

        // Read tag to well/sensor mappings from Kudu
        Map<String, String> kuduOptions = new HashMap<>();
        kuduOptions.put("kudu.master", arguments.get("kudu_master_hosts"));
        kuduOptions.put("kudu.table", "tag_mappings");
        this.tagMappingsDF = sqlContext.read()
                .format("org.apache.kudu.spark.kudu")
                .options(kuduOptions)
                .load();
    }

    @Override
    public TransformResult transform(JavaRDD<Record> records) {

        // SparkSQL needed for processing raw sensor data
        SQLContext sqlContext = new SQLContext(javaSparkContext);

        // Create an empty errors JavaPairRDD
        JavaRDD<Tuple2<Record,String>> emptyRDD = javaSparkContext.emptyRDD();
        JavaPairRDD<Record, String> errors = JavaPairRDD.fromJavaRDD(emptyRDD);

        // Convert RDD to dataframe of Sensor class
        JavaRDD<Sensor> sensorRDD = records.map(
                new Function<Record, Sensor>() {
                    @Override
                    public Sensor call(Record record) throws Exception {
                        Sensor sensor = new Sensor();
                        sensor.setRecordTime(record.get("/record_time").getValueAsString());
                        sensor.setTagID(record.get("/tag_id").getValueAsInteger());
                        sensor.setValue(record.get("/value").getValueAsDouble());

                        return sensor;
                    }
                });

        // Read in raw sensor data from Streamsets
        DataFrame sensorDF = sqlContext.createDataFrame(sensorRDD, Sensor.class)
                .withColumnRenamed("tagID", "tag_id")
                .withColumnRenamed("recordTime", "record_time");

        // Join sensor dataframe with tag mappings from kudu
        DataFrame taggedSensorDF = sensorDF.join(tagMappingsDF, "tag_id")
            .select("record_time", "well_id", "sensor_name", "value");

        // Pivot tagged sensor dataframe with a column for each entity
        DataFrame pivottedSensorDF = taggedSensorDF.groupBy("record_time", "well_id")
                .pivot("sensor_name")
                .agg(avg("value"));

        // Extract column names for writing back to streamsets
        final String[] columns = pivottedSensorDF.columns();

        // Convert dataframe back to RDD of <Record> to pass back to Streamsets
        JavaRDD<Record> result = pivottedSensorDF.javaRDD().map(
                new Function<Row, Record>() {
                    @Override
                    public Record call(Row row) throws Exception {
                        Record newRecord = RecordCreator.create();
                        Map<String, Field> map = new HashMap<>();

                        // We don't know how many columns the pivot will generate
                        // so we set everything that's not "record_time" or "well_id"
                        // to DOUBLE type as defined in Kudu
                        for (int i = 0; i < columns.length; i++) {
                            String column = columns[i];
                            if (column.equals("record_time")) {
                                map.put(column, Field.create(row.getString(i)));
                            } else if (column.equals("well_id")) {
                                map.put(column, Field.create(row.getInt(i)));
                            } else {
                                if (!row.isNullAt(i))
                                    map.put(column, Field.create(row.getDouble(i)));
                            }
                        }

                        newRecord.set(Field.create(map));
                        return newRecord;

                    }
                });

        return new TransformResult(result, errors);
    }
}
