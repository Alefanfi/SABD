package queries;

import org.apache.commons.collections.IteratorUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.io.Serializable;
import java.text.ParseException;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class Query2 {

    private static final String outputPath = "hdfs://namenode:9000/spark/query2/";
    private static final String inputPath = "hdfs://namenode:9000/data/somministrazione-vaccini.parquet";

    private static final Logger log = LogManager.getLogger(Query2.class.getName());

    public static void main(String[] args) throws ParseException {

        SimpleDateFormat year_month_day_format = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat year_month_format = new SimpleDateFormat("yyyy-MM");
        Date start_date = year_month_day_format.parse("2021-1-31");

        Tuple3Comparator<String, String, String> comp = new Tuple3Comparator<>(Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder());

        SparkSession spark = SparkSession
                .builder()
                .appName("Query2")
                .master("spark://spark:7077")
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Instant start = Instant.now();

        //Create dataset from file parquet "somministrazione-vaccini.parquet"
        JavaPairRDD<Tuple3<String, String, String>, Tuple2<Long, Long>> grouped_rdd = spark.read().parquet(inputPath)
                .toJavaRDD()
                .filter(row -> year_month_day_format.parse(row.getString(0)).after(start_date)) // date > 2021-1-31
                .mapToPair(
                    row -> {
                        Date date = year_month_day_format.parse(row.getString(0));
                        return new Tuple2<>(new Tuple3<>(date, row.getString(3), row.getString(2)), Long.valueOf(row.getString(5))); //((date, age, region), num_vaccinated_women)
                    })
                .reduceByKey(Long::sum) // Adding up the number of women vaccinated in a region during a specific date
                .mapToPair(
                    row -> {
                        String year_month = year_month_format.format(row._1._1());
                        Long convertedDate = row._1._1().getTime();
                        return new Tuple2<>(new Tuple3<>(year_month, row._1._2(), row._1._3()), new Tuple2<>(convertedDate, row._2)); //(year_month, age, region), (date, num_vaccinated_women)
                    });


        //Per la risoluzione della query, considerare le sole categorie per cui nel mese solare
        // in esame vengono registrati almeno due giorni di campagna vaccinale.
        JavaPairRDD<Tuple3<String, String, String>, Integer> x = grouped_rdd
                .keys()
                .mapToPair(key -> new Tuple2<>(key, 1))
                .reduceByKey(Integer::sum);

        Broadcast<List<Tuple3<String, String, String>>> noDuplicati = sc
                .broadcast(x.filter(row -> row._2 < 2)
                .keys()
                .collect());

        grouped_rdd = grouped_rdd
                .filter(row -> !noDuplicati.value().contains(row._1))
                .sortByKey(comp, true);


        List<Tuple2<Tuple3<String,String,String>,Iterable<Tuple2<Long, Long>>>> list = grouped_rdd
                .groupByKey() //((year_month, age, region), [](date, num_vaccinated_women))
                .collect();

        List<Tuple2<Long, Tuple3<String, String, String>>> prediction = new ArrayList<>();

        for (Tuple2<Tuple3<String, String, String>, Iterable<Tuple2<Long, Long>>> tuple3IterableTuple2 : list) {

            Tuple3<String, String, String> key = tuple3IterableTuple2._1; // (year_month, age, region)
            Date date = year_month_format.parse(key._1() + "-" + "01");

            List<Tuple2<Long, Long>> data = IteratorUtils.toList(tuple3IterableTuple2._2.iterator()); // [](date, num_vaccinated_women)

            Encoder<Tuple2<Long, Long>> encoder = Encoders.tuple(Encoders.LONG(), Encoders.LONG());

            //Create Dataset
            Dataset<Row> dataset = spark.createDataset(data, encoder)
                    .toDF("data", "vaccini");

            dataset.show();

            VectorAssembler assembler = new VectorAssembler()
                    .setInputCols(new String[]{"data"})
                    .setOutputCol("features");

            LinearRegression lr = new LinearRegression()
                    .setMaxIter(10)
                    .setRegParam(0.3)
                    .setElasticNetParam(0.8)
                    .setFeaturesCol("features")
                    .setLabelCol("vaccini");

            // Fit the model
            LinearRegressionModel lrModel = lr.fit(assembler.transform(dataset));

            // Get next month for prediction
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.MONTH, 1);
            Date newdate = cal.getTime();

            // Predict women vaccinations the first day of the next month
            double predict = lrModel.predict(Vectors.dense(newdate.getTime()));

            prediction.add(new Tuple2<>((long) predict, key)); // (num_women_vacc_next_month, (year_month, age, region))

        }

        Encoder<Tuple2<Long, Tuple3<String, String, String>>> encoder2 = Encoders.tuple(Encoders.LONG(), Encoders.tuple
                (Encoders.STRING(), Encoders.STRING(), Encoders.STRING()));

        JavaPairRDD<Tuple3<String, String, Long>, String> predictPairRDD = spark
                .createDataset(prediction, encoder2)
                .toDF("key", "value")
                .selectExpr("key as Vaccini_Predetti",  "value._1 as Date", "value._2 as Age", "value._3 Area")
                .toJavaRDD()
                .mapToPair(
                    row ->
                        new Tuple2<>(new Tuple2<>(row.getString(1), row.getString(2)), new Tuple2<>(row.getLong(0), row.getString(3))) // ((year_month, age),(num_women_vacc, region))
                    )
                .groupByKey() // ((year_month, age), [](num_women_vacc, region))
                .flatMapToPair(
                    input -> {

                        List<Tuple2<Long,String>> scores = IteratorUtils.toList(input._2.iterator());
                        scores.sort(Comparator.comparing(n -> n._1)); // Sorting by num_women_vacc

                        List<Tuple2<Tuple3<String, String, Long>, String>> newlist = new ArrayList<>();

                        String date = input._1._1;
                        String age = input._1._2;

                        for(int i=0; i<5; i++){

                            Tuple2<Long,String> tupla = scores.get(i);

                            newlist.add(new Tuple2<>(new Tuple3<>(date, age, tupla._1), tupla._2)); // ((year_month, age, num_women_vacc), region)
                        }
                        return newlist.iterator();
                    })
                .sortByKey(new Tuple3Comparator<>(Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<Long>naturalOrder()), true); // Sorts by date, age and num_women_vacc


        Encoder<Tuple2<Tuple3<String, String, Long>, String>> encoder3 = Encoders.tuple(Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.LONG()), Encoders.STRING());

        Dataset<Row> output_dt = spark.createDataset(JavaPairRDD.toRDD(predictPairRDD), encoder3)
                .toDF("key", "value")
                .selectExpr("key._1 as date", "key._2 as age", "value as region", "key._3 as vacc_women");

        output_dt.show();

        output_dt.write().mode(SaveMode.Overwrite).option("header","true").csv(outputPath);

        Instant end = Instant.now();
        log.info("Query completed in " + Duration.between(start, end).toMillis() + " ms");

        spark.close();

    }
}

class Tuple3Comparator<tuple1, tuple2, tuple3> implements Comparator<Tuple3<tuple1, tuple2, tuple3>>, Serializable {

    private static final long serialVersionUID = 1L;
    private final Comparator<tuple1> tuple1;
    private final Comparator<tuple2> tuple2;
    private final Comparator<tuple3> tuple3;

    public Tuple3Comparator(Comparator<tuple1> tuple1, Comparator<tuple2> tuple2, Comparator<tuple3> tuple3){
        this.tuple2 = tuple2;
        this.tuple1 = tuple1;
        this.tuple3 = tuple3;
    }

    @Override
    public int compare(Tuple3<tuple1, tuple2, tuple3> o1, Tuple3<tuple1, tuple2, tuple3> o2) {
        int res = this.tuple1.compare(o1._1(), o2._1());
        int res2 = this.tuple2.compare(o1._2(), o2._2());
        int res3 = this.tuple3.compare(o1._3(), o2._3());
        if(res == 0){
            if(res2 == 0){
                return res3;
            }else {
                return res2;
            }
        } return res;
    }
}