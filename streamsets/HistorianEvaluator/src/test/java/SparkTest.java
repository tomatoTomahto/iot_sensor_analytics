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
import org.apache.spark.SparkConf;

// Kudu Libraries
import org.apache.kudu.spark.kudu.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by samir on 2017-01-26.
 */
public class SparkTest {
    public static class Mapping implements Serializable {
        private int tag_id;
        private String well_name;
        private String entity_name;

        public int getTagID() {return tag_id;}
        public String getWellName() {return well_name;}
        public String getEntityName() {return entity_name;}

        public void setTagID(int tag_id) {this.tag_id=tag_id;}
        public void setWellName(String well_name) {this.well_name=well_name;}
        public void setEntityName(String entity_name) {this.entity_name=entity_name;}
    }

    public static class Sensor implements Serializable {
        private double value;
        private int tag_id;
        private long record_time;

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }

        public int getTagID() {
            return tag_id;
        }

        public void setTagID(int tag_id) {
            this.tag_id = tag_id;
        }

        public long getRecordTime() {
            return record_time;
        }

        public void setRecordTime(long record_time) {
            this.record_time = record_time;
        }
    }

    public static void main(String [] args){
        SparkConf conf = new SparkConf()
                .setAppName("test historian")
                .setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqc = new SQLContext(sc);

        // Read tag mappings from Kudu
        Map<String, String> kuduOptions = new HashMap<>();
        kuduOptions.put("kudu.master", "localhost");
        kuduOptions.put("kudu.table", "tag_entities");
        DataFrame mappingsDF = sqc.read()
                .format("org.apache.kudu.spark.kudu")
                .options(kuduOptions)
                .load();
        mappingsDF.show();

        // Read sensor data from disk
        DataFrame sensorDF = sqc.read().json("/Users/samir/Box Sync/Cloudera/Demos/cdh_historian/tmp/sensor2.json");
        sensorDF.show();

        // Join sensor data with tag entity mappings
        DataFrame taggedDF = sensorDF.join(mappingsDF, "tag_id")
                .select(col("record_time"),
                        col("entity_name"), col("well_name"),
                        col("value"),
                        round((unix_timestamp(col("record_time"))
                                .plus(150L).divide(300L)))
                                .multiply(300L).cast("timestamp")
                            .alias("adj_record_time"));
        taggedDF.show();
        //.withColumn("adjRecordTime",
                //        round(col("recordTime").plus(150L).divide(300L)).multiply(300L));
        ;

        // Pivot tagged sensor data and aggregate values
        DataFrame pivotDF = taggedDF.groupBy("adj_record_time", "well_name")
                .pivot("entity_name")
                .agg(avg("value"));
        pivotDF.show();

        String[] columns = pivotDF.columns();
        for (int i = 0; i<columns.length; i++) {
            String column = columns[i];
            if (column.equals("adj_record_time")){
                System.out.println("column:" + i + " name:" + column + " type:long");
            } else if (columns.equals("well_name")) {
                System.out.println("column:" + i + " name:" + column + " type:string");
            } else {
                System.out.println("column:" + i + " name:" + column + " type:double");
            }
        }

        System.out.println(Double.valueOf(null));
    }
}
