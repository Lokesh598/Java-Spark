package com.lokesh.learning.processors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadCsvSaveToMonoDB {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("CsvMonogoKafka")
                .getOrCreate();

        Dataset<Row> csvData = sparkSession.read()
                .format("csv")
                .option("header",true)
                .load("spark.car.csv");

        csvData = csvData.withColumn("_id", org.apache.spark.sql.functions.struct(csvData.col("_id").alias("oid")));

        csvData.show();

        csvData.write()
                .format("mongo")
                .option("uri", "mongodb://localhost:27017/spark.error_collection")
                .save();

        sparkSession.stop();
    }
}
