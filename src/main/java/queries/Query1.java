package queries;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Query1 {

    private static final String outputPath = "hdfs://namenode:9000/spark/output2";
    private static final String pathHDFS = "hdfs://namenode:9000/data";
    private static final String inputPath = "/somministrazione-vaccini-summary.parquet";
    private static final String inputPath2 = "/punti-somministrazione.parquet";
    private static final Logger log = LogManager.getLogger(Query1.class.getName());

    public static void main(String[] args ) throws ParseException {

        Date start_date = new SimpleDateFormat("yyyy-MM-dd").parse("2020-12-31");

        TupleComparator<Date, String> comp = new TupleComparator<>(Comparator.<Date>naturalOrder(), Comparator.<String>naturalOrder());

        SparkSession spark = SparkSession
                .builder()
                .appName("Prova")
                .master("spark://spark:7077")
                .getOrCreate();

        log.info("Starting processing");

        //Create dataset from file parquet "somministrazione-vaccini-summary.parquet"
        Dataset<Row> row = spark.read().parquet(pathHDFS + inputPath);
        JavaRDD<Row> rdd = row.toJavaRDD();

        //Elimino i dati relativi al mese di dicembre 2020
        JavaRDD<Row> rrd_2021 = rdd.filter(x -> new SimpleDateFormat("yyyy-MM-dd").parse(x.getString(0)).after(start_date));

        //JavaPairRDD can be used for operations which require an explicit Key field.
        /*I valori vengono ordinati secondo la data (il giorno viene impostato sempre come il primo giorno del mese
        * tanto abbiamo bisogno dei valori che riguardano tutti i giorni di un determinato mese)*/
        JavaPairRDD<Date, Tuple2<String, Long>> summary = rrd_2021.mapToPair((x -> {
            Date date = new SimpleDateFormat("yyyy-MM").parse(x.getString(0));
            Long total = Long.valueOf(x.getString(2));
            return new Tuple2<>(date, new Tuple2<>(x.getString(1), total));
        })).sortByKey(true).cache();

        /*Modificata la chiava, posta uguale alla coppia Data+Regione
        *Vengono raggruppati i valori relativi alle persone vaccinate per un determinato mese e una determinata regione*/
        JavaPairRDD<Tuple2<Date, String>, Long> meseArea = summary.mapToPair((x) ->
                new Tuple2<>(new Tuple2<>(x._1, x._2._1), x._2._2)).reduceByKey(Long::sum);


        JavaPairRDD<String, Tuple2<Date, Long>> area = meseArea.mapToPair((x) ->
                new Tuple2<>(x._1._2, new Tuple2<>(x._1._1, x._2)));

        //Create dataset from file parquet "punti-somministrazione-tipologia.parquet"
        Dataset<Row> dataset = spark.read().parquet(pathHDFS + inputPath2);
        JavaRDD<Row> rdd2 = dataset.toJavaRDD();

        JavaPairRDD<String, Integer> centri = rdd2.mapToPair((x) ->
                new Tuple2<>(x.getString(0), 1)).reduceByKey(Integer::sum);

        JavaPairRDD<String, Tuple2<Tuple2<Date, Long>, Integer>> meseAreaCentri = area.join(centri);

        JavaPairRDD<Tuple2<Date, String>, Long> out = meseAreaCentri.mapToPair((x ->
                new Tuple2<>(new Tuple2<>(x._2._1._1, x._1), x._2._1._2/x._2._2))).sortByKey(comp);

        JavaPairRDD<Tuple2<String, String>, Long> out2 = out.mapToPair((x ->{
            String convertedDate = new SimpleDateFormat("yyyy-MM-dd").format(x._1._1);
            return new Tuple2<>(new Tuple2<>(convertedDate, x._1._2), x._2);
        }));

        //out.saveAsTextFile(outputPath);

        List<Tuple2<Tuple2<String, String>, Long>> result = out2.collect();
        log.info("Result:");
        for (Tuple2<Tuple2<String, String>, Long> value: result) {
            log.info(value);
        }

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
