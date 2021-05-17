package queries;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class Query1 {

    private static final String outputPath = "hdfs://namenode:9000/spark/output2";
    private static final String inputPath = "hdfs://namenode:9000/data/somministrazione-vaccini-summary.parquet";
    private static final Logger log = LogManager.getLogger(Query1.class.getName());

    public static void main(String[] args ) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Prova")
                .master("spark://spark:7077")
                .getOrCreate();

        log.info("Starting processing query1");

        Dataset<Row> allrow = spark.read().parquet(inputPath);
        JavaRDD<Row> rdd = allrow.toJavaRDD();

        //JavaPairRDD can be used for operations which require an explicit Key field.
        JavaPairRDD<Date, Tuple2<String, Long>> summary = rdd.mapToPair((row -> {
            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(row.getString(0));
            return new Tuple2<>(date, new Tuple2<>(row.getString(1), row.getLong(2)));
        })).sortByKey(true).cache();



        //rdd.saveAsTextFile(outputPath);

        spark.close();



    }
}
