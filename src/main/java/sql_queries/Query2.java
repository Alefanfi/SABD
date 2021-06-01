package sql_queries;

import comparator.Tuple3Comparator;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.util.*;


public class Query2 {

    private static final String inputPath = "hdfs://namenode:9000/data/somministrazione-vaccini.parquet";

    public static void main(String[] args) {

        SimpleDateFormat year_month_day_format = new SimpleDateFormat("dd-MM-yyyy");

        SparkSession spark = SparkSession
                .builder()
                .appName("Query2")
                .master("spark://spark:7077")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().parquet(inputPath);

        dataset.createOrReplaceTempView("dati");

        //take all value after the 31-01-2021
        Dataset<Row> sqlDF = spark.sql("SELECT data_somministrazione AS date, nome_area AS area, fascia_anagrafica AS age, sesso_femminile " +
                "FROM dati WHERE DATE(data_somministrazione) > DATE('2021-1-31')");

        sqlDF = sqlDF.withColumn("sesso_femminile", sqlDF.col("sesso_femminile").cast("long"))
                .groupBy("date", "area", "age")
                .sum("sesso_femminile");

        //create a new column with month and year value
        sqlDF = sqlDF.withColumn("month_year", functions.concat(
                functions.month(sqlDF.col("date")),
                functions.lit("-"),
                functions.year(sqlDF.col("date"))));

        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("date",  DataTypes.StringType, true),
                DataTypes.createStructField("region",  DataTypes.StringType, true),
                DataTypes.createStructField("age",  DataTypes.StringType, true),
                DataTypes.createStructField("predicted", DataTypes.IntegerType, true)
        });


        Dataset<Row> prediction_dt = sqlDF
                .withColumn("date", sqlDF.col("date").cast("timestamp").cast("long"))
                .withColumn("vaccini", sqlDF.col("sum(sesso_femminile)").cast("long"))
                .withColumn("value", functions.struct("date", "vaccini"))
                .drop(sqlDF.col("sum(sesso_femminile)"))
                .sort("month_year", "area", "age")
                .groupBy("month_year", "area", "age")
                .agg(functions.collect_list("value"))//(mont_year, area, age) [](data, vaccini)
                .map((MapFunction<Row, Row>) row->{

                    List<Row> data = row.getList(3);

                    SimpleRegression simpleRegression = new SimpleRegression();
                    data.forEach(x -> simpleRegression.addData(x.getLong(0)*1000, x.getLong(1)));

                    // Get next month for prediction
                    Date date = year_month_day_format.parse("01-0" + row.getString(0));
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(date);
                    cal.add(Calendar.MONTH, 1);
                    Date nextMonth = cal.getTime();
                    String nextMonthString = year_month_day_format.format(nextMonth);

                    double predict = simpleRegression.predict((double) nextMonth.getTime());

                    return RowFactory.create(nextMonthString, row.getString(1), row.getString(2), (int) Math.round(predict)); //date, region, age, predict

                }, RowEncoder.apply(schema));

        JavaPairRDD<Tuple3<String, String, Integer>, String> predict_rdd = prediction_dt
                .toJavaRDD()
                .mapToPair(
                        row ->
                                new Tuple2<>(new Tuple2<>(row.getString(0), row.getString(2)), new Tuple2<>(row.getInt(3), row.getString(1))) // ((year_month, age),(num_women_vac, region))
                )
                .groupByKey()//((year_month, age), [](num_women_vacc, region))
                .flatMapToPair(
                        x -> {

                            List<Tuple2<Integer, String>> scores = IteratorUtils.toList(x._2.iterator());
                            scores.sort(Comparator.comparing(n -> n._1)); // Sorting by num_women_vacc
                            Collections.reverse(scores);

                            List<Tuple2<Tuple3<String, String, Integer>, String>> newlist = new ArrayList<>();

                            String date = x._1._1;
                            String age = x._1._2;

                            for(int i=0; i<5; i++){

                                Tuple2<Integer, String> tupla = scores.get(i);

                                newlist.add(new Tuple2<>(new Tuple3<>(date, age, tupla._1), tupla._2)); // ((year_month, age, num_women_vacc), region)
                            }
                            return newlist.iterator();
                        })
                .sortByKey(new Tuple3Comparator<>(Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<Integer>reverseOrder()), true); // Sorts by date, age and num_women_vacc

        Dataset<Row> output_dt = spark.createDataset(JavaPairRDD.toRDD(predict_rdd), Encoders.tuple(Encoders.tuple
                (Encoders.STRING(), Encoders.STRING(), Encoders.INT()), Encoders.STRING()))
                .toDF("key", "value")
                .selectExpr("key._1 as date", "key._2 as age", "value as region", "key._3 as vacc_women");

        output_dt.show();

        spark.close();
    }
}
