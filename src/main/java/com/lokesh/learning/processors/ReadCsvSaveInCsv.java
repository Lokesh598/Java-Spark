package com.lokesh.learning.processors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadCsvSaveInCsv {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("CsvMonogoKafka")
                .getOrCreate();

        try {
            Dataset<Row> csvData = sparkSession.read()
                    .format("csv")
                    .option("header",true)
                    .load("spark.car1.csv");

//        csvData = csvData.withColumn("_id", org.apache.spark.sql.functions.struct(csvData.col("_id").alias("oid")));
            csvData.show();

            csvData.write()
                    .format("csv")
                    .option("header", true)
                    .mode("overwrite")
                    .save("car2.csv");



        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sparkSession.stop();
        }

    }
}
