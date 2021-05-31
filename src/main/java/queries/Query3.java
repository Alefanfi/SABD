package queries;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;

import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class Query3 {

    private static final String vaccini_summary = "hdfs://namenode:9000/data/somministrazione-vaccini-summary.parquet";
    private static final String tot_popolazione = "hdfs://namenode:9000/data/totale-popolazione.parquet";
    private static final String outputPath = "hdfs://namenode:9000/spark/query3/";

    public static void main(String[] args ) throws ParseException{

        int algo = Integer.parseInt(args[0]); // Choosen algorithm
        int k = Integer.parseInt(args[1]); // Number of clusters

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


        //Create dataset from file parquet "totale-popolazione.parquet"
        Dataset<Row> row = spark.read().parquet(tot_popolazione);
        JavaPairRDD<String, Long> pop_rdd = row
                .toJavaRDD()
                .mapToPair(
                        x -> {
                            String region = x.getString(0).split("/")[0]; // Removing part of the region name after the /
                            Long pop = Long.valueOf(x.getString(1));

                            return new Tuple2<>(region, pop); // (region, population)
                        });


        //Create dataset from file parquet "somministrazione-vaccini-summary.parquet"
        JavaRDD<Row> vacc_summary = spark.read().parquet(vaccini_summary)
                .toJavaRDD()
                .filter(x -> year_month_day_format.parse(x.getString(0)).after(start_date)); // data_somministrazione > 2020-12-26


        JavaPairRDD<String, Long> vacc_rdd = vacc_summary
                .mapToPair(
                        x ->{
                            String region = x.getString(1).split("/")[0]; // Removing part of the region name after the /
                            Long num_vacc = Long.valueOf(x.getString(2));
                            return new Tuple2<>(region, num_vacc); // (region, number_of_vaccinations)
                        })
                .reduceByKey(Long::sum); // (region, total)


        JavaPairRDD<String, Tuple2<String, Long>> predict_rdd =  vacc_summary
                .mapToPair(
                        x -> {
                            Long num_vacc = Long.valueOf(x.getString(2));
                            Long date = year_month_day_format.parse(x.getString(0)).getTime();
                            String region = x.getString(1).split("/")[0]; // Removing part of the region name after the /
                            return new Tuple2<>(region, new Tuple2<>(date, num_vacc)); // (region,(date, number_of_vaccinations))
                        })
                .groupByKey() // Grouping by region   (region, [](date, number_of_vaccinations))
                .mapToPair(
                        x-> {
                            SimpleRegression reg = new SimpleRegression();
                            x._2.iterator().forEachRemaining( y -> reg.addData(y._1, y._2));

                            Long res = (long) reg.predict(tomorrow.getTime());

                            return new Tuple2<>(x._1, new Tuple2<>(year_month_day_format.format(tomorrow), res)); // (region, (date, predicted_vacc))
                        });


        JavaPairRDD<String, Tuple2<String, Double>> vacc_pop = vacc_rdd
                .join(predict_rdd) // (region, (total, (date, predicted_vacc)))
                .join(pop_rdd) // (region, ((total,(date, predicted_vacc)), population))
                .mapToPair(
                        x -> {
                            Double percent = (((double) x._2._1._1 + x._2._1._2._2)/x._2._2)*100;
                            return new Tuple2<>(x._1, new Tuple2<>(x._2._1._2._1, percent)); // (region, (date, percent_vaccinated))
                        })
                .sortByKey(); // Order by region name


        Encoder<Tuple2<String, Tuple2<String, Double>>> encoder = Encoders.tuple(Encoders.STRING(), Encoders.tuple(Encoders.STRING(), Encoders.DOUBLE()));
        Dataset<Row> output_dt = spark.createDataset(JavaPairRDD.toRDD(vacc_pop), encoder)
                .toDF("key", "value")
                .selectExpr("key as region", "value._1 as date", "value._2 as percent");

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"percent"})
                .setOutputCol("features");

        Dataset<Row> output = assembler.transform(output_dt);

        output.show();

        if(algo == 1){

            // Trains a bisecting k-means model.
            BisectingKMeans bkm = new BisectingKMeans().setK(k).setSeed(1);
            BisectingKMeansModel bmodel = bkm.fit(output);

            // Make predictions
            Dataset<Row> bpredictions = bmodel
                    .transform(output) // predicting clusters
                    .select("date","region", "percent", "prediction"); // Selecting output data columns

            bpredictions.write().mode(SaveMode.Overwrite).option("header", "true").csv(outputPath +"biseckmeans/");
        }
        else{
            // Trains a k-means model.
            KMeans kmeans = new KMeans().setK(k).setSeed(1L);
            KMeansModel model = kmeans.fit(output);

            // Make predictions
            Dataset<Row> predictions = model
                    .transform(output)  // predicting clusters
                    .select("date","region", "percent", "prediction"); // Selecting output data columns

            predictions.write().mode(SaveMode.Overwrite).option("header", "true").csv(outputPath +"kmeans/");
        }

        spark.close();
    }
}
