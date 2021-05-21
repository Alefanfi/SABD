package queries;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import scala.Serializable;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class Query1 {

    private static final String outputPath = "hdfs://namenode:9000/spark/query1";
    private static final String vaccini_summary = "hdfs://namenode:9000/data/somministrazione-vaccini-summary.parquet";
    private static final String punti_somministrazione = "hdfs://namenode:9000/data/punti-somministrazione.parquet";
    private static final Logger log = LogManager.getLogger(Query1.class.getName());

    public static void main(String[] args ) throws ParseException {

        SimpleDateFormat year_month_day_format = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat year_month_format = new SimpleDateFormat("yyyy-MM");
        TupleComparator<String, String> comp = new TupleComparator<>(Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder());

        Date start_date = year_month_day_format.parse("2020-12-31");

        SparkSession spark = SparkSession
                .builder()
                .appName("Prova")
                .master("spark://spark:7077")
                .getOrCreate();

        log.info("Processing query 1");

        Instant start = Instant.now();

        //Create dataset from file parquet "somministrazione-vaccini-summary.parquet"
        Dataset<Row> row = spark.read().parquet(vaccini_summary);
        JavaRDD<Row> rdd = row.toJavaRDD().cache();

        JavaPairRDD<String, Tuple2<Date, Long>> vaccini = rdd
                .filter(x -> year_month_day_format.parse(x.getString(0)).after(start_date)) // data_somministrazione > 2020-12-31
                .mapToPair(
                    x -> {
                        Date date = year_month_format.parse(x.getString(0)); // Keeping only year and month
                        Tuple2<Date, String> key = new Tuple2<>(date, x.getString(1));
                        Long total = Long.valueOf(x.getString(2));
                        return new Tuple2<>(key, total); // ((date, region_name), num_vaccinated_people)
                    })
                .reduceByKey(Long::sum) // Adding up the number of people vaccinated in a region during a specific month
                .mapToPair(x -> new Tuple2<>(x._1._2, new Tuple2<>(x._1._1, x._2))); // (region_name, (date, num_vaccinated_people))


        //Create dataset from file parquet "punti-somministrazione.parquet"
        Dataset<Row> dataset = spark.read().parquet(punti_somministrazione);
        JavaRDD<Row> rdd2 = dataset.toJavaRDD();

        JavaPairRDD<String, Integer> centri = rdd2
                .mapToPair(x -> new Tuple2<>(x.getString(0), 1)) // ( region_name , 1 )
                .reduceByKey(Integer::sum);  // Adding up the number of vaccination centers in a single region


        JavaPairRDD<Tuple2<String, String>, Double> output = vaccini
                .join(centri)   // Joining with centri by region_name (region_name,((date, num_vaccinated_people), num_vac_centers))
                .mapToPair(
                    x -> {
                        String date = new SimpleDateFormat("yyyy-MM").format(x._2._1._1);
                        Double mean = Double.valueOf(x._2._1._2)/x._2._2;   // Mean number of vaccinations per center
                        return new Tuple2<>(new Tuple2<>(date, x._1), mean); // (date, (region_name, mean))
                    })
                .sortByKey(comp, true);


        Encoder<Tuple2<Tuple2<String, String>, Double>> encoder = Encoders.tuple(Encoders.tuple(Encoders.STRING(), Encoders.STRING()), Encoders.DOUBLE());

        Dataset<Row> output_dt = spark.createDataset(JavaPairRDD.toRDD(output), encoder)
                .toDF("key", "value")
                .selectExpr("key._1 as anno_mese", "key._2 as regione", "value as media_vaccinazioni");

        output_dt.write().mode(SaveMode.Overwrite).parquet(outputPath);
        Instant end = Instant.now();
        log.info("Completed in " + Duration.between(start, end).toMillis() + " ms");
        spark.close();

    }
}


class TupleComparator<tuple1, tuple2> implements Comparator<Tuple2<tuple1, tuple2>>, Serializable {

    private static final long serialVersionUID = 1L;
    private final Comparator<tuple1> tuple1;
    private final Comparator<tuple2> tuple2;

    public TupleComparator(Comparator<tuple1> tuple1, Comparator<tuple2> tuple2){
        this.tuple2 = tuple2;
        this.tuple1 = tuple1;
    }

    @Override
    public int compare(Tuple2<tuple1, tuple2> o1, Tuple2<tuple1, tuple2> o2) {
        if (tuple1.compare(o1._1, o2._1) == 0){
            return this.tuple2.compare(o1._2, o2._2);
        } else{
            return this.tuple1.compare(o1._1, o2._1);
        }
    }

}
