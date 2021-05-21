package queries;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Query3 {

    private static final String vaccini_summary = "hdfs://namenode:9000/data/somministrazione-vaccini-summary.parquet";
    private static final String tot_popolazione = "hdfs://namenode:9000/data/totale-popolazione.parquet";
    private static final String outputPath = "hdfs://namenode:9000/spark/query3";
    private static final Logger log = LogManager.getLogger(Query3.class.getName());

    public static void main(String[] args ) throws ParseException {

        SimpleDateFormat year_month_day_format = new SimpleDateFormat("yyyy-MM-dd");

        Date start_date = year_month_day_format.parse("2020-12-26");

        SparkSession spark = SparkSession
                .builder()
                .appName("Query 3")
                .master("spark://spark:7077")
                .getOrCreate();

        log.info("Starting processing");

        //Create dataset from file parquet "somministrazione-vaccini-summary.parquet"
        Dataset<Row> row = spark.read().parquet(vaccini_summary);
        JavaRDD<Row> rdd = row.toJavaRDD();

        JavaPairRDD<String, Long> tot_vaccinati = rdd
            .filter(x -> year_month_day_format.parse(x.getString(0)).after(start_date)) // data_somministrazione > 2020-12-26
            .mapToPair( x -> new Tuple2<>(x.getString(1), x.getLong(2))) // (region, number_of_vaccinations)
            .reduceByKey(Long::sum); // Adding up the number of people vaccinated in a region


        //Create dataset from file parquet "somministrazione-vaccini-summary.parquet"
        Dataset<Row> row2 = spark.read().parquet(tot_popolazione);
        JavaRDD<Row> rdd2 = row2.toJavaRDD();

        JavaPairRDD<String, Long> popolazione = rdd2
                .mapToPair( x -> new Tuple2<>(x.getString(0), x.getLong(1))); // (region, population)


        JavaPairRDD<String, Double> vacc_pop = tot_vaccinati
                .join(popolazione) // Joining with popolazione by region (region, (num_of_vaccinations, population))
                .mapToPair(
                    x -> {
                        Double percent = (Double.valueOf(x._2._1)/x._2._2)*100;
                        return new Tuple2<>(x._1, percent); // (region, percent_vaccinated)
                    });


        Encoder<Tuple2<String, Double>> encoder = Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE());

        Dataset<Row> output_dt = spark.createDataset(JavaPairRDD.toRDD(vacc_pop), encoder)
                .toDF("key", "value")
                .selectExpr("key as regione", "value as percentuale_vaccinati");

        StructType schema = new StructType(new StructField[]{
                new StructField("regione", DataTypes.StringType, false, Metadata.empty()),
                new StructField("percentuale_vaccinati", DataTypes.DoubleType, false, Metadata.empty())
        });

        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");


        // Trains a k-means model.
        KMeans kmeans = new KMeans().setK(2).setSeed(1L);
        KMeansModel model = kmeans.fit(output_dt);

        // Make predictions
        Dataset<Row> predictions = model.transform(output_dt);

        // Evaluate clustering by computing Silhouette score
        ClusteringEvaluator evaluator = new ClusteringEvaluator();

        double silhouette = evaluator.evaluate(predictions);

        // Shows the result.
        Vector[] centers = model.clusterCenters();
        log.info("Cluster centers: ");
        for (Vector center: centers) {
            log.info(center);
        }

        spark.close();
    }
}
