package queries;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Query3 {

    private static final String pathHDFS = "hdfs://namenode:9000/data";
    private static final String inputPath = "/somministrazione-vaccini-summary.parquet";
    private static final Logger log = LogManager.getLogger(Query3.class.getName());

    public static void main(String[] args ) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Prova")
                .master("spark://spark:7077")
                .getOrCreate();

        log.info("Starting processing");

        //Create dataset from file parquet "somministrazione-vaccini-summary.parquet"
        Dataset<Row> row = spark.read().parquet(pathHDFS + inputPath);
        JavaRDD<Row> rdd = row.toJavaRDD();
    }
}
