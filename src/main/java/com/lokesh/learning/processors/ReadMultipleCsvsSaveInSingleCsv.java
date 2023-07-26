package com.lokesh.learning.processors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class ReadMultipleCsvsSaveInSingleCsv {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("CsvMonogoKafka")
                .getOrCreate();

        Dataset<Row> csvData = sparkSession.read()
                .format("csv")
                .option("header",true)
                .load("csv");

        csvData.show();

//        Dataset<Row> singlePartitionData = csvData.coalesce(1);
/**
 * repartition() is preferable instead of coalesce()
 */
        csvData.repartition(1).write()
                .format("csv")
                .option("header", true)
                .save("car");
        sparkSession.stop();
    }
}

