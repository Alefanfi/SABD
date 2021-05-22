package queries;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.hdfs.protocol.proto.EncryptionZonesProtos;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Query3 {

    private static final String vaccini_summary = "hdfs://namenode:9000/data/somministrazione-vaccini-summary.parquet";
    private static final String tot_popolazione = "hdfs://namenode:9000/data/totale-popolazione.parquet";
    private static final String outputPath = "hdfs://namenode:9000/spark/query3";
    private static final Logger log = LogManager.getLogger(Query3.class.getName());

    public static <DataFrame> void main(String[] args ) throws ParseException {

        SimpleDateFormat year_month_day_format = new SimpleDateFormat("yyyy-MM-dd");
        Date start_date = year_month_day_format.parse("2020-12-26");

        SparkSession spark = SparkSession
                .builder()
                .appName("Query 3")
                .master("spark://spark:7077")
                .getOrCreate();

        log.info("Processing query 3");

        Instant start = Instant.now();

        //Create dataset from file parquet "totale-popolazione.parquet"
        Dataset<Row> row = spark.read().parquet(tot_popolazione);

        row.show();

        JavaPairRDD<String, Long> popolazione = row
                .toJavaRDD()
                .mapToPair(
                        x -> {
                            String region = x.getString(0).split("/")[0];
                            Long pop = Long.valueOf(x.getString(1));

                            return new Tuple2<>(region, pop); // (region, population)
                        });

        // Extracting region names from dataset row
        List<Row> regions =  row.select("area").distinct().toJavaRDD().collect();

        //Create dataset from file parquet "somministrazione-vaccini-summary.parquet"
        Dataset<Row> row2 = spark.read().parquet(vaccini_summary);
        JavaRDD<Row> rdd = row2.toJavaRDD();

        for (int i=0; i<regions.size(); i++) {

            int index = i;
            JavaPairRDD<Date, Long> region_rdd = rdd
                    .filter(x -> year_month_day_format.parse(x.getString(0)).after(start_date) // data_somministrazione > 2020-12-26;
                            && x.getString(1).compareTo(regions.get(index).toString()) == 0) // filter by region
                    .mapToPair(
                        x -> {
                            Long num_vacc = Long.valueOf(x.getString(2));
                            Date date = year_month_day_format.parse(x.getString(1));
                            return new Tuple2<>(date, num_vacc); // (date, number_of_vaccinations)
                        });

            Encoder<Tuple2<java.sql.Date, Long>> encoder = Encoders.tuple(Encoders.DATE(), Encoders.LONG());

            /*Dataset<Row> regression_dt = spark.createDataset(JavaPairRDD.toRDD(region_rdd), encoder)
                    .toDF("key", "value")
                    .selectExpr("key as datae", "value as vaccini");*/




        }




/*

        JavaPairRDD<String,Tuple2<String, Long>> tot_vaccinati = rdd
                .filter(x -> year_month_day_format.parse(x.getString(0)).after(start_date)) // data_somministrazione > 2020-12-26
                .mapToPair(
                        x -> {
                            Long num_vacc = Long.valueOf(x.getString(2));
                            String region = x.getString(1).replaceAll("[^a-zA-Z0-9]", " ");
                            return new Tuple2<>(region, new Tuple2<>(x.getString(0), num_vacc)); // (region,(date, number_of_vaccinations))
                        });

        Encoder<Tuple2<String,Tuple2<String, Long>>> encoder1 = Encoders.tuple(Encoders.STRING(), Encoders.tuple(Encoders.STRING(), Encoders.LONG()));

        Dataset<Row> regression_dt = spark.createDataset(JavaPairRDD.toRDD(tot_vaccinati), encoder1)
                .toDF("key", "value")
                .selectExpr("key as regione", "value._1 as data", "value._2 as vaccini");

        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(new String[]{"regione", "data"})
                .setOutputCol("features");

        Dataset<Row> training = assembler1.transform(regression_dt);

        LinearRegression lr = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8)
                .setFeaturesCol("features")
                .setLabelCol("vaccini");

        // Fit the model.
        LinearRegressionModel lrModel = lr.fit(training);

        Dataset<Row> testing = row
                .select(row.col("nome_area"))
                .distinct()
                .withColumn("data", functions.lit("2020-06-01"));

        Dataset<Row> test = assembler1.transform(testing);

        //Predict
        Dataset<Row> results = lrModel.transform(test);

        results.show();

        //Create dataset from file parquet "somministrazione-vaccini-summary.parquet"
        Dataset<Row> row2 = spark.read().parquet(tot_popolazione);
        JavaRDD<Row> rdd2 = row2.toJavaRDD();

        JavaPairRDD<String, Long> popolazione = rdd2
                .mapToPair(
                    x -> {
                        String region = x.getString(0).replaceAll("[^a-zA-Z0-9]", " ");
                        Long pop = Long.valueOf(x.getString(1));

                        return new Tuple2<>(region, pop); // (region, population)
                    });


        JavaPairRDD<String, Double> vacc_pop = tot_vaccinati
                .join(popolazione) // Joining with popolazione by region (region, (num_of_vaccinations, population))
                .mapToPair(
                    x -> {
                        Double percent = (Double.valueOf(x._2._1)/x._2._2)*100;
                        return new Tuple2<>(x._1, percent); // (region, percent_vaccinated)
                    });

        List<Tuple2<String, Double>> res2 = vacc_pop.collect();

        for (Tuple2<String, Double> re : res2) {
            log.info(re._1 + "|");
        }


        /*

        Encoder<Tuple2<String, Double>> encoder = Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE());

        Dataset<Row> output_dt = spark.createDataset(JavaPairRDD.toRDD(vacc_pop), encoder)
                .toDF("key", "value")
                .selectExpr("key as regione", "value as percentuale_vaccinati");

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"percentuale_vaccinati"})
                .setOutputCol("features");

        Dataset<Row> output = assembler.transform(output_dt);

        // Trains a k-means model.
        KMeans kmeans = new KMeans().setK(4).setSeed(1L);
        KMeansModel model = kmeans.fit(output);

        // Make predictions
        Dataset<Row> predictions = model.transform(output);

        // Shows the result.
        Vector[] centers = model.clusterCenters();
        log.info("Cluster centers: ");
        for (Vector center: centers) {
            log.info(center);
        }

        predictions.show();

         */

        Instant end = Instant.now();
        log.info("Query completed in " + Duration.between(start, end).toMillis() + " ms");

        spark.close();
    }
}
