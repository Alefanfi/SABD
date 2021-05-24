package queries;

import org.apache.commons.collections.IteratorUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class Query3 {

    private static final String vaccini_summary = "hdfs://namenode:9000/data/somministrazione-vaccini-summary.parquet";
    private static final String tot_popolazione = "hdfs://namenode:9000/data/totale-popolazione.parquet";
    private static final String outputPath = "hdfs://namenode:9000/spark/query3/";

    private static final Logger log = LogManager.getLogger(Query3.class.getName());

    public static void main(String[] args ) throws ParseException {

        SimpleDateFormat year_month_day_format = new SimpleDateFormat("yyyy-MM-dd");
        Date start_date = year_month_day_format.parse("2020-12-26");

        // Tomorrow
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, 1);
        Date tomorrow = calendar.getTime();

        SparkSession spark = SparkSession
                .builder()
                .appName("Query 3")
                .master("spark://spark:7077")
                .getOrCreate();

        log.info("Processing query 3");

        Instant start = Instant.now();

        //Create dataset from file parquet "totale-popolazione.parquet"
        Dataset<Row> row = spark.read().parquet(tot_popolazione);
        JavaPairRDD<String, Long> popolazione = row
                .toJavaRDD()
                .mapToPair(
                        x -> {
                            String region = x.getString(0).split("/")[0]; // Removing part of the region name after the /
                            Long pop = Long.valueOf(x.getString(1));

                            return new Tuple2<>(region, pop); // (region, population)
                        });


        //Create dataset from file parquet "somministrazione-vaccini-summary.parquet"
        Dataset<Row> row2 = spark.read().parquet(vaccini_summary);
        JavaPairRDD<String, Tuple2<Long, Long>> vacc_rdd =  row2
                .toJavaRDD()
                .filter(x -> year_month_day_format.parse(x.getString(0)).after(start_date)) // data_somministrazione > 2020-12-26
                .mapToPair(
                        x -> {
                            Long num_vacc = Long.valueOf(x.getString(2));
                            Long date = year_month_day_format.parse(x.getString(0)).getTime();
                            String region = x.getString(1).split("/")[0]; // Removing part of the region name after the /
                            return new Tuple2<>(region, new Tuple2<>(date, num_vacc)); // (region,(date, number_of_vaccinations))
                        });

        // Grouping up by region
        List<Tuple2<String, Iterable<Tuple2<Long, Long>>>> vacc = vacc_rdd
                .groupByKey() // (region, [](date, number_of_vaccinations))
                .collect();

        // Summing up total vaccinations
        JavaPairRDD<String, Long> tot_vac = vacc_rdd
                .mapToPair( x -> new Tuple2<>(x._1, x._2._2)) // (region, number_of_vaccinations)
                .reduceByKey(Long::sum); // summing up total of vaccinations for a region


        List<Tuple2<String, Long>> tomorrow_vacc = new ArrayList<>();

        for (Tuple2<String, Iterable<Tuple2<Long, Long>>> stringIterableTuple2 : vacc) {

            String region = stringIterableTuple2._1;
            List<Tuple2<Long, Long>> list = IteratorUtils.toList(stringIterableTuple2._2.iterator());

            Encoder<Tuple2<Long, Long>> encoder = Encoders.tuple(Encoders.LONG(), Encoders.LONG());
            Dataset<Row> regression_dt = spark.createDataset(list, encoder)
                    .toDF("key", "value")
                    .selectExpr("key as data", "value as vaccini");

            VectorAssembler assembler = new VectorAssembler()
                    .setInputCols(new String[]{"data"})
                    .setOutputCol("features");

            Dataset<Row> training = assembler.transform(regression_dt);

            LinearRegression lr = new LinearRegression()
                    .setMaxIter(10)
                    .setRegParam(0.3)
                    .setElasticNetParam(0.8)
                    .setFeaturesCol("features")
                    .setLabelCol("vaccini");

            // Fit the model
            LinearRegressionModel lrModel = lr.fit(training);

            //Predict tomorrow's vaccinations
            double results = lrModel.predict(Vectors.dense(tomorrow.getTime()));

            tomorrow_vacc.add(new Tuple2<>(region, (long) results)); // (region, num_vacc_tomorrow)

        }

        JavaPairRDD<String, Long> tomorrow_vacc_rdd = spark
                .createDataset(tomorrow_vacc, Encoders.tuple(Encoders.STRING(), Encoders.LONG()))
                .toJavaRDD()
                .mapToPair( x -> new Tuple2<>(x._1, x._2)); // (region, num_vacc_tomorrow)

        JavaPairRDD<String, Double> vacc_pop = tot_vac
                .join(tomorrow_vacc_rdd) // (region, (tot_vacc, tomorrow_vacc))
                .join(popolazione) // (region, ((tot_vacc, tomorrow_vacc), population))
                .mapToPair(
                        x -> {
                            Double percent = (((double) x._2._1._1 + x._2._1._2)/x._2._2)*100;
                            return new Tuple2<>(x._1, percent); // (region, percent_vaccinated)
                        })
                .sortByKey(); // Order by region name

        Encoder<Tuple2<String, Double>> encoder = Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE());

        Dataset<Row> output_dt = spark.createDataset(JavaPairRDD.toRDD(vacc_pop), encoder)
                .toDF("key", "value")
                .selectExpr("key as regione", "value as percentuale_vaccinati");

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"percentuale_vaccinati"})
                .setOutputCol("features");

        Dataset<Row> output = assembler.transform(output_dt);

        // Trains a k-means model.
        KMeans kmeans = new KMeans().setK(2).setSeed(1L);
        KMeansModel model = kmeans.fit(output);

        // Make predictions
        Dataset<Row> predictions = model
                .transform(output)  // predicting clusters
                .select("regione", "percentuale_vaccinati", "prediction"); // Selecting output data columns

        // Trains a bisecting k-means model.
        BisectingKMeans bkm = new BisectingKMeans().setK(2).setSeed(1);
        BisectingKMeansModel bmodel = bkm.fit(output);

        // Make predictions
        Dataset<Row> bpredictions = bmodel
                .transform(output) // predicting clusters
                .select("regione", "percentuale_vaccinati", "prediction"); // Selecting output data columns



        // Writing results to HDFS
        predictions.write().mode(SaveMode.Overwrite).option("header", "true").csv(outputPath +"kmeans/"+ year_month_day_format.format(tomorrow));
        bpredictions.write().mode(SaveMode.Overwrite).option("header", "true").csv(outputPath +"biseckmeans/"+ year_month_day_format.format(tomorrow));

        Instant end = Instant.now();
        log.info("Query completed in " + Duration.between(start, end).toMillis() + " ms");

        spark.close();
    }
}
