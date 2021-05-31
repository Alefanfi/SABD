package sql_queries;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import scala.Tuple4;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
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

        dataset.createOrReplaceTempView("dati");

        Dataset<Row> sqlDF = spark.sql("SELECT data_somministrazione, nome_area, fascia_anagrafica, sesso_femminile " +
                "FROM dati WHERE DATE(data_somministrazione) > DATE('2021-1-31')");

        sqlDF = sqlDF.withColumn("sesso_femminile", sqlDF.col("sesso_femminile").cast("long"))
                .groupBy("data_somministrazione", "nome_area", "fascia_anagrafica").sum("sesso_femminile");

        sqlDF = sqlDF.withColumn("mese_anno", functions.concat(
                functions.month(sqlDF.col("data_somministrazione")
                ), functions.lit("-"), functions.year(sqlDF.col("data_somministrazione"))));

        sqlDF = sqlDF.withColumn("key", functions
                .concat(sqlDF.col("mese_anno"), functions.lit(" - "),
                        sqlDF.col("fascia_anagrafica"), functions.lit(" - "), sqlDF.col("nome_area")))
                .sort("key");

        List<String> keyString = sqlDF
                .select(sqlDF.col("key"))
                .distinct()
                .collectAsList()
                .stream()
                .map(r -> r.getString(0))
                .collect(Collectors.toList());

        List<Tuple4<String, String, String, Double>> prediction = new ArrayList<>();

        log.info("-------------------------------------------------------------------------" + keyString.size());

        for (String s : keyString) {

            Dataset<Row> datasetLR = sqlDF.filter(sqlDF.col("key").equalTo(s))
                    .withColumn("data_somministrazione", sqlDF.col("data_somministrazione").cast("timestamp").cast("long"))
                    .sort("data_somministrazione");

            VectorAssembler assembler = new VectorAssembler()
                    .setInputCols(new String[]{"data_somministrazione"})
                    .setOutputCol("features");

            LinearRegression lr = new LinearRegression()
                    .setMaxIter(1)
                    .setRegParam(0.3)
                    .setElasticNetParam(0.8)
                    .setFeaturesCol("features")
                    .setLabelCol("sum(sesso_femminile)");

            // Fit the model
            LinearRegressionModel lrModel = lr.fit(assembler.transform(datasetLR));

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

            prediction.add(new Tuple4<>(newdateString, tmp[2], tmp[4], predict));

        }

        for(Tuple4<String, String, String, Double> m : prediction){
            log.info(m);
        }


        spark.close();
    }
}
