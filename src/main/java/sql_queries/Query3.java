package sql_queries;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;

import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class Query3 {

    private static final String vaccini_summary = "hdfs://namenode:9000/data/somministrazione-vaccini-summary.parquet";
    private static final String tot_popolazione = "hdfs://namenode:9000/data/totale-popolazione.parquet";
    private static final String outputPath = "hdfs://namenode:9000/spark/sql-query3/";


    public static void main(String[] args ) {

        int algo = Integer.parseInt(args[0]); // Choosen algorithm
        int k = Integer.parseInt(args[1]); // Number of clusters

        SimpleDateFormat year_month_day_format = new SimpleDateFormat("yyyy-MM-dd");

        // Tomorrow
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, 1);
        Date tomorrow = calendar.getTime();

        SparkSession spark = SparkSession
                .builder()
                .appName("SQL Query 3")
                .master("spark://spark:7077")
                .getOrCreate();


        //Create dataset from file parquet "totale-popolazione.parquet"
        Dataset<Row> pop_dt = spark.read().parquet(tot_popolazione)
                .toDF("region", "population") // (region, pop)
                .withColumn("population", functions.col("population").cast("long"));


        //Create dataset from file parquet "somministrazione-vaccini-summary.parquet"
        Dataset<Row> vacc_summary = spark.read().parquet(vaccini_summary)
                .toDF("date", "region", "num_vacc") // (date, region, vacc)
                .where("date > '2020-12-26'");

        vacc_summary.createOrReplaceTempView("summary");


        Dataset<Row> vacc_dt = spark.sql("SELECT region, CAST(SUM(num_vacc) AS BIGINT) FROM summary GROUP BY region"); // (region, total)


        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("region",  DataTypes.StringType, true),
                DataTypes.createStructField("date",  DataTypes.StringType, true),
                DataTypes.createStructField("predicted", DataTypes.LongType, true)
        });

        StructType schema2 = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("region",  DataTypes.StringType, true),
                DataTypes.createStructField("date",  DataTypes.StringType, true),
                DataTypes.createStructField("percent", DataTypes.DoubleType, true)
        });


        Dataset<Row> predicted_dt = spark.sql("SELECT region, CAST( CAST( date AS TIMESTAMP ) AS BIGINT) AS date, CAST(num_vacc AS BIGINT) AS num_vacc FROM summary")
                .withColumn("date_vacc", functions.struct("date", "num_vacc")) // (region, date, vacc, (date, vacc))
                .groupBy("region")
                .agg(functions.collect_list("date_vacc")) // (region, [](date, vacc))
                .map((MapFunction<Row, Row>) row ->{

                    List<Row> data = row.getList(1);

                    SimpleRegression reg = new SimpleRegression();
                    data.forEach( x -> reg.addData(x.getLong(0), x.getLong(1)));

                    Long res = (long) reg.predict(tomorrow.getTime());

                    return RowFactory.create(row.getString(0), year_month_day_format.format(tomorrow), res); // (region, tomorrow, predicted)

                }, RowEncoder.apply(schema))
                .join(vacc_dt, "region") // (region, tomorrow, predicted, total)
                .join(pop_dt, "region") // (region, tomorrow, predicted, total, pop)
                .map((MapFunction<Row, Row>) row -> {

                    Double percent = (((double) row.getLong(2) + row.getLong(3) )/ row.getLong(4))*100;

                    return RowFactory.create(row.getString(0), row.getString(1), percent); // (region, date, percent)

                }, RowEncoder.apply(schema2));


        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"percent"})
                .setOutputCol("features");

        Dataset<Row> output = assembler.transform(predicted_dt);

        output.show();

        if(algo == 1){

            // Trains a bisecting k-means model.
            BisectingKMeans bkm = new BisectingKMeans().setK(k).setSeed(1);
            BisectingKMeansModel bmodel = bkm.fit(output);

            // Make predictions
            Dataset<Row> bpredictions = bmodel
                    .transform(output) // predicting clusters
                    .select("date","region", "percent", "prediction"); // Selecting output data columns

            bpredictions.repartition(1).write().mode(SaveMode.Overwrite).option("header", "true").csv(outputPath +"biseckmeans/");
        }
        else{
            // Trains a k-means model.
            KMeans kmeans = new KMeans().setK(k).setSeed(1L);
            KMeansModel model = kmeans.fit(output);

            // Make predictions
            Dataset<Row> predictions = model
                    .transform(output)  // predicting clusters
                    .select("date","region", "percent", "prediction"); // Selecting output data columns

            predictions.repartition(1).write().mode(SaveMode.Overwrite).option("header", "true").csv(outputPath +"kmeans/");
        }


        spark.close();
    }

}
