package queries;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;


public class Query1 {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main( String[] args ) {

/*
        SparkSession spark = SparkSession.builder().appName("Query 1").master("spark://spark:7077").config("spark.dynamicAllocation.enabled","false").getOrCreate();

        Dataset<Row> row = spark.read().text("C:/Users/eilen/IdeaProjects/SABD/data/prova.txt");
        JavaRDD<Row> rdd = row.toJavaRDD();

        List<Row> list = rdd.collect();
        for (Row elem: list) {
            System.out.println(elem);
        }

        spark.close();


 */

        String outputPath = "hdfs://namenode:9000/spark/output5";
        if (args.length > 0)
            outputPath = args[0];

        SparkConf conf = new SparkConf()
                .setMaster("spark://spark:7077")
                .setAppName("Hello World");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*
         * As data source, we can use a file stored on the local file system
         * or on the HDFS, or we can parallelize
         */
//        JavaRDD<String> input = sc.textFile("hdfs://HOST:PORT/input");
//        JavaRDD<String> input = sc.textFile("input");
        JavaRDD<String> input = sc.parallelize(Arrays.asList(
                "if you prick us do we not bleed",
                "if you tickle us do we not laugh",
                "if you poison us do we not die and",
                "if you wrong us shall we not revenge"
        ));

        // Transformations
        JavaRDD<String> words = input.flatMap(line -> Arrays.asList(SPACE.split(line)).iterator());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((x, y) -> x+y);

        // Action
        /* Trasformations are lazy, and they are applied only when a action
         * should be performed on a RDD.                                            */
        counts.saveAsTextFile(outputPath);

        sc.stop();

    }
}
