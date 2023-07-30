package com.lokesh.learning;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class WordCounter {
    public static void main(String[] args) {
        // Create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("WordCounter")
                .master("local[*]")
                .getOrCreate();

        // Read input text file into a DataFrame
        Dataset<Row> data = spark.read().text("input.txt");

        // Split the lines into words and explode into individual rows
        Dataset<Row> words = data.withColumn("word", explode(split(col("value"), " ")));

        // Group by word and count occurrences
        Dataset<Row> wordCounts = words.groupBy("word").count();

        // Show the word counts
        wordCounts.show();

        // Stop the SparkSession
        spark.stop();
    }
}
