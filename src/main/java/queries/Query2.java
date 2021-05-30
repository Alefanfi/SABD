package queries;

import comparator.Tuple3Comparator;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;

import scala.Tuple2;
import scala.Tuple3;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Query2 {

    private static final String outputPath = "hdfs://namenode:9000/spark/query2/";
    private static final String inputPath = "hdfs://namenode:9000/data/somministrazione-vaccini.parquet";

    public static void main(String[] args) throws ParseException {

        SimpleDateFormat year_month_day_format = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat year_month_format = new SimpleDateFormat("yyyy-MM");
        Date start_date = year_month_day_format.parse("2021-1-31");

        SparkSession spark = SparkSession
                .builder()
                .appName("Query2")
                .master("spark://spark:7077")
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());


        //Create dataset from file parquet "somministrazione-vaccini.parquet"
        JavaPairRDD<Tuple3<String, String, String>, Tuple2<Long, Long>> vacc_rdd = spark.read().parquet(inputPath)
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
                        return new Tuple2<>(new Tuple3<>(year_month, row._1._2(), row._1._3()), new Tuple2<>(convertedDate, row._2)); //(year_month, age, region), (date, tot_vaccinated_women)
                    });


        // Only months with at least two days of vaccinations will be considered ---------------------------------------
        JavaPairRDD<Tuple3<String, String, String>, Integer> grouped_rdd = vacc_rdd
                .keys()
                .mapToPair(key -> new Tuple2<>(key, 1)) // ((year_month, age, region), 1)
                .reduceByKey(Integer::sum);

        Broadcast<List<Tuple3<String, String, String>>> noDup = sc
                .broadcast(grouped_rdd.filter(row -> row._2 < 2)
                .keys()
                .collect());

        vacc_rdd = vacc_rdd
                .filter(row -> !noDup.value().contains(row._1));
        //--------------------------------------------------------------------------------------------------------------


        JavaPairRDD<Tuple3<String, String, Long>, String> vacc_predicted_rdd = vacc_rdd
                .groupByKey() //((year_month, age, region), [](date, num_vaccinated_women))
                .mapToPair(
                        x ->{
                            SimpleRegression reg = new SimpleRegression();
                            x._2.iterator().forEachRemaining( y -> reg.addData(y._1, y._2));

                            // Get next month for prediction
                            Date date = year_month_format.parse(x._1._1() + "-" + "01");
                            Calendar cal = Calendar.getInstance();
                            cal.setTime(date);
                            cal.add(Calendar.MONTH, 1);
                            Date nextmonth = cal.getTime();

                            Long res = (long) reg.predict(nextmonth.getTime());

                            return new Tuple2<>(new Tuple2<>(year_month_day_format.format(nextmonth), x._1._2()), new Tuple2<>(res, x._1._3())); // ((year_month, age),(num_women_vacc, region))
                        })
                .groupByKey() // ((year_month, age), [](num_women_vacc, region))
                .flatMapToPair(
                        input -> {

                            List<Tuple2<Long,String>> scores = IteratorUtils.toList(input._2.iterator());
                            scores.sort(Comparator.comparing(n -> n._1)); // Sorting by num_women_vacc
                            Collections.reverse(scores); // Dec order

                            List<Tuple2<Tuple3<String, String, Long>, String>> newlist = new ArrayList<>();

                            String date = input._1._1;
                            String age = input._1._2;

                            for(int i=0; i<5; i++){

                                Tuple2<Long,String> tupla = scores.get(i);

                                newlist.add(new Tuple2<>(new Tuple3<>(date, age, tupla._1), tupla._2)); // ((year_month, age, num_women_vacc), region)
                            }
                            return newlist.iterator();
                        })
                .sortByKey(new Tuple3Comparator<>(Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<Long>reverseOrder()), true); // Sorts by date, age
                                                                                                                                                                          // and num_women_vacc


        Encoder<Tuple2<Tuple3<String, String, Long>, String>> encoder3 = Encoders.tuple(Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.LONG()), Encoders.STRING());

        Dataset<Row> output_dt = spark.createDataset(JavaPairRDD.toRDD(vacc_predicted_rdd), encoder3)
                .toDF("key", "value")
                .selectExpr("key._1 as date", "key._2 as age", "value as region", "key._3 as vacc_women");

        output_dt.write().mode(SaveMode.Overwrite).option("header","true").csv(outputPath);

        spark.close();

    }
}