package sql_queries;

import comparator.Tuple3Comparator;

import org.apache.commons.collections.IteratorUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class Query2 {

    private static final String outputPath = "hdfs://namenode:9000/spark/query2/";
    private static final String inputPath = "hdfs://namenode:9000/data/somministrazione-vaccini.parquet";

    private static final Logger log = LogManager.getLogger(sql_queries.Query2.class.getName());

    public static void main(String[] args) throws ParseException {

        SimpleDateFormat year_month_day_format = new SimpleDateFormat("dd-MM-yyyy");

        SparkSession spark = SparkSession
                .builder()
                .appName("SQL Query2")
                .master("spark://spark:7077")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().parquet(inputPath);

        dataset.createOrReplaceTempView("data");

        Dataset<Row> sqlDF = spark.sql("SELECT data_somministrazione, nome_area, fascia_anagrafica, CAST(sesso_femminile AS INT) AS sesso_femminile " +
                "FROM data WHERE DATE(data_somministrazione) > DATE('2021-1-31')");

        sqlDF = sqlDF
                .groupBy("data_somministrazione", "nome_area", "fascia_anagrafica") // Grouping by date, region, age
                .sum("sesso_femminile") // Sum up all vaccinations in a single day
                .withColumn("mese_anno", functions.concat(

                    // Creating new column with only year and month of date
                    functions.month(sqlDF.col("data_somministrazione")),
                    functions.lit("-"),
                    functions.year(sqlDF.col("data_somministrazione")))
                )
                .withColumn("key", functions.concat(

                        // Creating key with date-age-region
                        sqlDF.col("mese_anno"), functions.lit(" - "),
                        sqlDF.col("fascia_anagrafica"), functions.lit(" - "), sqlDF.col("nome_area"))
                )
                .withColumn("data_somministrazione", sqlDF.col("data_somministrazione").cast("timestamp").cast("long")) // Casting date to long
                .withColumn("vaccini", sqlDF.col("sum(sesso_femminile)").cast("long")) // Casting vacc number to long
                .drop("sum(sesso_femminile)", "nome_area", "fascia_anagrafica")
                .sort("key", "data_somministrazione");

        sqlDF.show();

        List<String> keyString = sqlDF
                .select(sqlDF.col("key"))
                .distinct()
                .collectAsList()
                .stream()
                .map(r -> r.getString(0)).sorted(String.CASE_INSENSITIVE_ORDER).collect(Collectors.toList());

        List<Tuple4<String, String, String, Long>> prediction = new ArrayList<>();

        for (String s : keyString) {

            Dataset<Row> dt = sqlDF.filter(sqlDF.col("key").equalTo(s));

            VectorAssembler assembler = new VectorAssembler()
                    .setInputCols(new String[]{"data_somministrazione"})
                    .setOutputCol("features");

            LinearRegression lr = new LinearRegression()
                    .setMaxIter(5)
                    .setRegParam(0.3)
                    .setElasticNetParam(0.8)
                    .setFeaturesCol("features")
                    .setLabelCol("vaccini");

            // Fit the model
            LinearRegressionModel lrModel = lr.fit(assembler.transform(dt));

            //Get current date
            String[] tmp = s.split(" ");
            Date date = year_month_day_format.parse("01-0" + tmp[0]);

            // Get next month for prediction
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.MONTH, 1);
            Date newdate = cal.getTime();
            String newdateString = year_month_day_format.format(newdate);

            // Predict women vaccinations the first day of the next month
            double predict = lrModel.predict(Vectors.dense(newdate.getTime()));

            prediction.add(new Tuple4<>(newdateString, tmp[2], tmp[4], (long) predict));

        }

        Encoder<Tuple4<String, String, String, Long>> encoder = Encoders.tuple(Encoders.STRING(),
                Encoders.STRING(), Encoders.STRING(), Encoders.LONG());

        JavaPairRDD<Tuple3<String, String, Integer>, String> predictRDD = spark
                .createDataset(prediction, encoder)
                .toDF("Date", "Age", "Area", "Vaccini")
                .toJavaRDD()
                .mapToPair(
                        row ->
                                new Tuple2<>(new Tuple2<>(row.getString(1), row.getString(2)), new Tuple2<>( row.getLong(0), row.getString(3))) // ((year_month, age),(num_women_vacc, region))
                )
                .groupByKey() // ((year_month, age), [](num_women_vacc, region))
                .flatMapToPair(
                        x -> {

                            List<Tuple2<Long,String>> scores = IteratorUtils.toList(x._2.iterator());
                            scores.sort(Comparator.comparing(n -> n._1)); // Sorting by num_women_vacc
                            Collections.reverse(scores);

                            List<Tuple2<Tuple3<String, String, Integer>, String>> newlist = new ArrayList<>();

                            String date = x._1._1;
                            String age = x._1._2;

                            for(int i=0; i<5; i++){

                                Tuple2<Long, String> tupla = scores.get(i);

                                newlist.add(new Tuple2<>(new Tuple3<>(date, age, tupla._1.intValue()), tupla._2)); // ((year_month, age, num_women_vacc), region)
                            }
                            return newlist.iterator();
                        })
                .sortByKey(new Tuple3Comparator<>(Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<Integer>reverseOrder()), true); // Sorts by date, age and num_women_vacc

        Encoder<Tuple2<Tuple3<String, String, Integer>, String>> encoder2 = Encoders.tuple(Encoders.tuple
                (Encoders.STRING(), Encoders.STRING(), Encoders.INT()), Encoders.STRING());

        Dataset<Row> output_dt = spark.createDataset(JavaPairRDD.toRDD(predictRDD), encoder2)
                .toDF("key", "value")
                .selectExpr("key._1 as date", "key._2 as age", "value as region", "key._3 as vacc_women");

        output_dt.write().mode(SaveMode.Overwrite).option("header","true").csv(outputPath);

        spark.close();
    }
}
