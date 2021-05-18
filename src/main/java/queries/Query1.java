package queries;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


public class Query1 {

    private static final String outputPath = "hdfs://namenode:9000/spark/output2";
    private static final String pathHDFS = "hdfs://namenode:9000/data";
    private static final String inputPath = "/somministrazione-vaccini-summary.parquet";
    private static final String inputPath2 = "/punti-somministrazione.parquet";
    private static final Logger log = LogManager.getLogger(Query1.class.getName());

    public static void main(String[] args ) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Prova")
                .master("spark://spark:7077")
                .getOrCreate();

        log.info("Starting processing");

        //Create dataset from file parquet "somministrazione-vaccini-summary.parquet"
        Dataset<Row> row = spark.read().parquet(pathHDFS + inputPath);
        JavaRDD<Row> rdd = row.toJavaRDD();

        //JavaPairRDD can be used for operations which require an explicit Key field.
        JavaPairRDD<Date, Tuple2<String, Long>> summary = rdd.mapToPair((x -> {
            DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            Date date = formatter.parse(x.getString(0));
            Long total = Long.valueOf(x.getString(2));
            return new Tuple2<>(date, new Tuple2<>(x.getString(1), total));
        })).sortByKey(true).cache();

        JavaPairRDD<Tuple2<Date, String>, Long> meseArea = summary.mapToPair((x) -> {
            Calendar cal = Calendar.getInstance();
            cal.setTime(x._1);
            cal.set(Calendar.DAY_OF_MONTH, 1);
            return new Tuple2<>(new Tuple2<>(cal.getTime(), x._2._1), x._2._2);
        }).reduceByKey(Long::sum);

        JavaPairRDD<String, Tuple2<Date, Long>> area = meseArea.mapToPair((x) ->
                new Tuple2<>(x._1._2, new Tuple2<>(x._1._1, x._2)));

        //Create dataset from file parquet "punti-somministrazione-tipologia.parquet"
        Dataset<Row> dataset = spark.read().parquet(pathHDFS + inputPath2);
        JavaRDD<Row> rdd2 = dataset.toJavaRDD();

        JavaPairRDD<String, Integer> centri = rdd2.mapToPair((x) ->
                new Tuple2<>(x.getString(0), 1)).reduceByKey(Integer::sum);

        JavaPairRDD<String, Tuple2<Tuple2<Date, Long>, Integer>> meseAreaCentri = area.join(centri).sortByKey(true);

        JavaPairRDD<Tuple2<String, String>, Long> out = meseAreaCentri.mapToPair((x ->{
            String convertedDate = new SimpleDateFormat("yyyy-MM-dd").format(x._2._1._1);
            return new Tuple2<>(new Tuple2<>(convertedDate, x._1), x._2._1._2/x._2._2);
        }));

        //out.saveAsTextFile(outputPath);

        List<Tuple2<Tuple2<String, String>, Long>> result = out.collect();
        log.info("Result:");
        for (Tuple2<Tuple2<String, String>, Long> value: result) {
            log.info(value);
        }

        spark.close();

    }
}
