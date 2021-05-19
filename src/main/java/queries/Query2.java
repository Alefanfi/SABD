package queries;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.text.ParseException;

import java.text.SimpleDateFormat;
import java.util.*;

public class Query2 {

    private static final String outputPath = "hdfs://namenode:9000/spark/query2/";
    private static final String inputPath = "hdfs://namenode:9000/data/somministrazione-vaccini.parquet";

    private static final Logger log = LogManager.getLogger(Query2.class.getName());
    //private static final DateTimeFormatter directory_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd/HH-mm");
    //private static final DateTimeFormatter date_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    //private static final Date start_date = Date.valueOf("2021-1-31");

    public static void main(String[] args) throws ParseException {

        Date start_date = new SimpleDateFormat("yyyy-MM-dd").parse("2021-1-31");
        Tuple3Comparator<Date, String, String> comp = new Tuple3Comparator<>(Comparator.<Date>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder());

        SparkSession spark = SparkSession
                .builder()
                .appName("Query2")
                .master("spark://spark:7077")
                .getOrCreate();


        Dataset<Row> dataset = spark.read().parquet(inputPath);
        JavaRDD<Row> rdd = dataset.toJavaRDD();

        /*
        JavaPairRDD<String, Iterable<Row>> grouped_rdd = rdd
                .filter(row -> Date.valueOf(row.getString(0)).after(start_date))
                .mapToPair(row -> {

                    LocalDate date = LocalDate.parse(row.getString(0), date_formatter);
                    String key = date.getYear() + "_" + date.getMonthValue() + "_" + row.getString(1) + "_" + row.getString(2);
                    Row value = RowFactory.create(row.getString(0), row.getString(1), row.getString(2), row.getString(4));

                    return new Tuple2<>(key, value);

                })
                .groupByKey();

        List<Tuple2<String, Iterable<Row>>> result2 = grouped_rdd.collect();
        for (Tuple2<String, Iterable<Row>> value: result2) {
            log.info(value);
        }*/

        JavaRDD<Row> parsedRDD = rdd.filter(row -> new SimpleDateFormat("yyyy-MM-dd").parse(row.getString(0)).after(start_date));

        JavaPairRDD<Tuple3<Date, String, String>, Long> grouped_rdd = parsedRDD.mapToPair(row -> {
            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(row.getString(0));
            return new Tuple2<>(new Tuple3<>(date, row.getString(1), row.getString(2)), Long.valueOf(row.getString(4)));
        }).reduceByKey(Long::sum);

        JavaPairRDD<Tuple3<String, String, String>, Tuple2<String, Long>> mese = grouped_rdd.mapToPair(row ->{
            Date date = row._1._1();
            String month = new SimpleDateFormat("MM").format(date);
            String convertedDate = new SimpleDateFormat("yyyy-MM-dd").format(row._1._1());
            return new Tuple2<>(new Tuple3<>(month, row._1._2(), row._1._3()), new Tuple2<>(convertedDate, row._2));});

        JavaPairRDD<Tuple3<String, String, String>, Iterable<Tuple2<String, Long>>> mese2 = mese.groupByKey().filter(row -> {
            Iterable<Tuple2<String, Long>> list = row._2;
            int i = 0;
            for(Tuple2<String, Long> x : list) {
                i += 1;
                if(i == 2){
                    return true;
                }
            } return false; });

        JavaPairRDD<Tuple3<String, String, String>, Tuple2<String, Long>> meseWithoutIterable = mese2.mapToPair(
                (PairFunction<Tuple2<Tuple3<String, String, String>, Iterable<Tuple2<String, Long>>>, Tuple3<String, String, String>, Tuple2<String, Long>>) input -> {
                    String month = input._1._1();
                    String regione = input._1._2();
                    String fasciaAnagrafica = input._1._3();
                    for(Tuple2<String, Long> tupla: input._2()){
                        return new Tuple2<>(new Tuple3<>(month, regione, fasciaAnagrafica), new Tuple2<>(tupla._1(), tupla._2()));
                    }
                    return null;
                }
        );

        Encoder<Tuple2<Tuple3<String, String, String>, Tuple2<String, Long>>> encoder = Encoders.tuple(Encoders.tuple
                (Encoders.STRING(), Encoders.STRING(), Encoders.STRING()), Encoders.tuple(Encoders.STRING(), Encoders.LONG()));

        Dataset<Row> dt = spark.createDataset(JavaPairRDD.toRDD(meseWithoutIterable), encoder)
                .toDF("key", "value");

        Dataset<Row> newDt = dt.selectExpr("key._1 as Mese", "key._2 as Regione", "key._3 as FasciaAnagrafica", "value._1 as Data", "value._2 as Totale");

        newDt.show();

/*
        //Regressione lineare
        LinearRegression lr = new LinearRegression();
        LinearRegressionModel model = lr.fit(newDt);

*/
      /*

        // Meglio farlo con i dataset ? Nella linear regression si può passare solo un dataset
        dataset.filter(dataset.col("data_somministrazione").gt(start_date))
                .select("data_somministrazione","nome_area", "fascia_anagrafica", "sesso_femminile");


        LinearRegression lr = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);


        String path = outputPath.concat(LocalDateTime.now().format(directory_formatter));
        log.info(path);
        //grouped_rdd.saveAsTextFile(path);*/

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
