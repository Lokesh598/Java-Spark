package com.lokesh.learning.processors;

import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

/**
 * dropDuplicates() == remove duplicates from dataframe
 * groupBy().
 * "count() > 1"
 */
class MultipleCsvsToKafkaAndCsvProcessor {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("FilterCommonDataFromMultipleFiles")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> csvData = spark.read()
                .format("csv")
                .option("header", true)
                .load("csv");

        Dataset<Row> commonData = csvData.groupBy("_id", "licensePlate", "make", "model", "year")
                .count()
                .filter("count == 3")
                .drop("count");
// dataset can contains any object types
//      Dataframe = Dataset of row


//        csvData.createOrReplaceTempView("lokesh");
//        spark.sql("")

        commonData.show();

        commonData.toJSON()
                .write()
                .format("kafka")
//                .outputMode("complete")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "commoncar")
                .save();
//                .start()
//                .awaitTermination();

        commonData.write()
                .format("csv")
                .option("header", "true")
                .save("output.csv");


        spark.stop();
    }
}
