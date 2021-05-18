package queries;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;


import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class Query2 {

    private static final String outputPath = "hdfs://namenode:9000/spark/query2/";
    private static final String inputPath = "hdfs://namenode:9000/data/somministrazione-vaccini.parquet";

    private static final Logger log = LogManager.getLogger(Query2.class.getName());
    private static final DateTimeFormatter directory_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd/HH-mm");
    private static final DateTimeFormatter date_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final Date start_date = Date.valueOf("2021-1-31");

    public static void main(String[] args){

        SparkSession spark = SparkSession
                .builder()
                .appName("Query2")
                .master("spark://spark:7077")
                .getOrCreate();


        Dataset<Row> dataset = spark.read().parquet(inputPath);
        JavaRDD<Row> rdd = dataset.toJavaRDD();


        JavaPairRDD<String, Iterable<Row>> grouped_rdd = rdd
                .filter(row -> Date.valueOf(row.getString(0)).after(start_date))
                .mapToPair(row -> {

                    LocalDate date = LocalDate.parse(row.getString(0), date_formatter);
                    String key = date.getYear() + "_" + date.getMonthValue() + "_" + row.getString(2) + "_" + row.getString(3);
                    Row value = RowFactory.create(row.getString(0), row.getString(2), row.getString(3), row.getString(5));

                    return new Tuple2<>(key, value);

                })
                .groupByKey();


        // Meglio farlo con i dataset ? Nella linear regression si può passare solo un dataset
        dataset.filter(dataset.col("data_somministrazione").gt(start_date))
                .select("data_somministrazione","nome_area", "fascia_anagrafica", "sesso_femminile");


        LinearRegression lr = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);


        String path = outputPath.concat(LocalDateTime.now().format(directory_formatter));
        log.info(path);
        grouped_rdd.saveAsTextFile(path);

        spark.close();
    }
}
