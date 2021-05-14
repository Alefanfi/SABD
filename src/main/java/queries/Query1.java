package queries;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

import javax.xml.crypto.Data;
import java.util.List;

public class Query1 {
    public static void main( String[] args ) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Query 1").master("spark://localhost:7077")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().parquet("/query/data/users.parquet");
        JavaRDD<Row> rdd = dataset.toJavaRDD();

        List<Row> list = rdd.collect();
        for (Row elem: list) {
            System.out.println(elem);
        }
        spark.close();

    }
}
