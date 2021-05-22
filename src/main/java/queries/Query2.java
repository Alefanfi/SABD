package queries;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.*;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.text.ParseException;

import java.text.SimpleDateFormat;
import java.util.*;

public class Query2 {

    //private static final String outputPath = "hdfs://namenode:9000/spark/query2/";
    private static final String inputPath = "hdfs://namenode:9000/data/somministrazione-vaccini.parquet";
    private static final Logger log = LogManager.getLogger(Query2.class.getName());

    public static void main(String[] args) throws ParseException {

        SimpleDateFormat year_month_day_format = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat year_month_format = new SimpleDateFormat("yyyy-MM");
        Date start_date = year_month_day_format.parse("2021-1-31");
        Tuple3Comparator<String, String, String> comp = new Tuple3Comparator<>(Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder());
        Tuple3Comparator<String, String, Long> comp2 = new Tuple3Comparator<>(Comparator.<String>naturalOrder(), Comparator.<String>naturalOrder(), Comparator.<Long>naturalOrder());


        SparkSession spark = SparkSession
                .builder()
                .appName("Query2")
                .master("spark://spark:7077")
                .getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> dataset = spark.read().parquet(inputPath);
        JavaRDD<Row> rdd = dataset.toJavaRDD();

        JavaPairRDD<Tuple3<String, String, String>, Tuple2<String, Long>> grouped_rdd = rdd
                .filter(row -> year_month_day_format.parse(row.getString(0)).after(start_date)) //filter date
                .mapToPair(row -> {
                    Date date = year_month_day_format.parse(row.getString(0));
                    return new Tuple2<>(new Tuple3<>(date, row.getString(2), row.getString(3)), Long.valueOf(row.getString(5))); //(date, region, age), num_vaccinated_women
                }).reduceByKey(Long::sum) // Adding up the number of women vaccinated in a region during a specific date
                .mapToPair(row -> {
                    String month = year_month_format.format(row._1._1());
                    String convertedDate = year_month_day_format.format(row._1._1());
                    return new Tuple2<>(new Tuple3<>(month, row._1._2(), row._1._3()), new Tuple2<>(convertedDate, row._2)); //(month, region, age), (date, num_vaccinated_women)
                }).groupByKey()
                .flatMapToPair(
                        (PairFlatMapFunction<Tuple2<Tuple3<String, String, String>, Iterable<Tuple2<String, Long>>>, Tuple3<String, String, String>, Tuple2<String, Long>>) input -> {
                            String month = input._1._1();
                            String regione = input._1._2();
                            String fasciaAnagrafica = input._1._3();
                            ArrayList<Tuple2<Tuple3<String, String, String>, Tuple2<String, Long>>> list = new ArrayList<>();
                            for(Tuple2<String, Long> tupla: input._2()){
                                Tuple2<Tuple3<String, String, String>, Tuple2<String, Long>> tmp =
                                        new Tuple2<>(new Tuple3<>(month, regione, fasciaAnagrafica), new Tuple2<>(tupla._1, tupla._2));
                                list.add(tmp);
                            }
                            return list.iterator();
                        });

        //Per la risoluzione della query, considerare le sole categorie per cui nel mese solare
        // in esame vengono registrati almeno due giorni di campagna vaccinale.

        JavaPairRDD<Tuple3<String, String, String>, Integer> x = grouped_rdd.keys().mapToPair(key -> new Tuple2<>(key, 1)).reduceByKey(Integer::sum);
        Broadcast<List<Tuple3<String, String, String>>> noDuplicati = sc.broadcast(x.filter(row -> row._2 < 2).keys().collect());

        grouped_rdd = grouped_rdd.filter(row -> !noDuplicati.value().contains(row._1)).sortByKey(comp, true);

        List<Tuple3<String, String, String>> keyList = grouped_rdd.groupByKey().sortByKey(comp, true).keys().collect();

        List<Tuple2<Long, Tuple3<String, String, String>>> prediction = new ArrayList<>();

        for (int i=0; i<keyList.size(); i++){
            int index = i;

            //Per ogni chiave mi costruisco il JavaPairRDD, così da considerare ogni volta solamente uno specifico mese
            // una specifica regione e una specifica fascia d'età
            JavaPairRDD<Tuple3<String, String, String>, Tuple2<Long, Long>> newRDD = grouped_rdd
                    .filter(row -> row._1._1().equals(keyList.get(index)._1()) && row._1._2().equals(keyList.get(index)._2())
                            && row._1._3().equals(keyList.get(index)._3()))
                    .mapToPair(row -> {
                        Date date = year_month_day_format.parse(row._2._1());
                        Long dateLong = date.getTime();
                        return new Tuple2<>(new Tuple3<>(row._1._1(), row._1._2(), row._1._3()), new Tuple2<>(dateLong, row._2._2()));
                    });

            Encoder<Tuple2<Tuple3<String, String, String>, Tuple2<Long, Long>>> encoder = Encoders.tuple(Encoders.tuple
                    (Encoders.STRING(), Encoders.STRING(), Encoders.STRING()), Encoders.tuple(Encoders.LONG(), Encoders.LONG()));

            //Create Dataset
            Dataset<Row> dt = spark.createDataset(JavaPairRDD.toRDD(newRDD), encoder)
                    .toDF("key", "value");

            Dataset<Row> newDt = dt.selectExpr("key._1 as Mese", "key._2 as Regione", "key._3 as FasciaAnagrafica", "value._1 as Data", "value._2 as Vaccini");

            VectorAssembler assembler = new VectorAssembler()
                    .setInputCols(new String[]{"Data"})
                    .setOutputCol("Features");

            Dataset<Row> training = assembler.transform(newDt);

            //Definisco la regressione lineare
            LinearRegression lr = new LinearRegression()
                    .setMaxIter(10)
                    .setRegParam(0.3)
                    .setElasticNetParam(0.8)
                    .setFeaturesCol("Features")
                    .setLabelCol("Vaccini");

            // Fit the model.
            LinearRegressionModel lrModel = lr.fit(training);

            //Definisco quale è il mese successivo per il quale devoeffettuare il calcolo
            Date date = year_month_format.parse(keyList.get(0)._1()+"-"+"01");
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.MONTH, 1);
            String d = year_month_day_format.format(cal.getTime());

            long day = year_month_day_format.parse(d).getTime(); //Definisco il giorno per cui devo effettuare la predizione

            double predict = lrModel.predict(Vectors.dense(day)); //predico il numero di vaccinati per il primo giorno del mese successivo

            //Salvo il valore ottenuto in una lista
            prediction.add(new Tuple2<>((long) predict, new Tuple3<>(d, keyList.get(index)._2(), keyList.get(index)._3())));

        }

        for(Tuple2<Long, Tuple3<String, String, String>> l : prediction ){
            log.info(l);
        }

        Encoder<Tuple2<Long, Tuple3<String, String, String>>> encoder2 = Encoders.tuple(Encoders.LONG(), Encoders.tuple
                (Encoders.STRING(), Encoders.STRING(), Encoders.STRING()));

        Dataset<Row> predictDT = spark.createDataset(prediction, encoder2).toDF("key", "value")
                .selectExpr("key as Vaccini_Predetti",  "value._1 as Date", "value._2 as Area", "value._3 Age")
                /*.drop("Vaccini_Predetti", "Area")*/;

        JavaRDD<Row> predictRow = predictDT.toJavaRDD();

        JavaPairRDD<Tuple3<String, String, Long>, String> predictPairRDD = predictRow.mapToPair(row ->{
            Long predictLong = Long.valueOf(row.getString(0));
            return new Tuple2<>(new Tuple3<>(row.getString(1), row.getString(3), predictLong), row.getString(2));
        }).sortByKey();











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
