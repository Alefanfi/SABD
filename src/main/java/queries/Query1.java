package queries;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;


public class Query1 {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main( String[] args ) {

        String outputPath = "hdfs://namenode:9000/spark/output2";
        String inputPath = "hdfs://namenode:9000/data/somministrazione-vaccini-summary.parquet";


        SparkSession spark = SparkSession.builder().appName("Prova").master("spark://spark:7077").getOrCreate();

        Dataset<Row> row = spark.read().parquet(inputPath);
        JavaRDD<Row> rdd = row.toJavaRDD();

        rdd.saveAsTextFile(outputPath);

        spark.close();
    }
}
