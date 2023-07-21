package com.lokesh.learning;


/**
 *  To read a CSV file using Spark and Java, you need to follow these steps:
 *
 *     Create a SparkSession: Start by creating a SparkSession, which serves as the entry point for working with Spark in your Java application.
 *
 *     Load the CSV File: Use the read method of the SparkSession to read the CSV file and load it into a DataFrame or Dataset.
 *
 *     Specify Options (Optional): Depending on your CSV file's structure and characteristics, you may need to specify various options such as the delimiter, header presence, schema, etc.
 *
 *     Process the Data: Once the CSV data is loaded into a DataFrame or Dataset, you can perform various data processing operations using Spark's DataFrame API or Dataset API.
 *
 *     Stop the SparkSession: After processing the data, don't forget to stop the SparkSession to release the resources.
 */

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadCSV {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("ReadCSVWithSpark")
                .master("local[*]")
                .getOrCreate();
//        Dataset<Row> csv = sparkSession.read().format("csv").option("header","true").load("");
        Dataset<Row> data = sparkSession.read()
                .option("header", "true") // If the CSV file has a header row
                .option("inferSchema", "true") // Infer the data types of columns
                .csv("spark.car.csv");
        data.show();
        // Stop the SparkSession
        sparkSession.stop();
    }
}
