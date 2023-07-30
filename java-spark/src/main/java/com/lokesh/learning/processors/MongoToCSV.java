package com.lokesh.learning.processors;



import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class MongoToCSV {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("MongoToCSV")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> mongoData = spark.read()
                .format("com.mongodb.spark.sql.DefaultSource")
                .option("uri", "mongodb://localhost:27017/spark.car")
                .load();

        mongoData.show();
        // Extract oid field from ObjectID and use it as a regular string
        Dataset<Row> modifiedData = mongoData.withColumn("_id", mongoData.col("_id").getField("oid").cast("string"));

        modifiedData.show();

        // Select the columns you want to save to CSV (excluding the oid field)
        Dataset<Row> selectedData = modifiedData.select("_id", "licensePlate","make", "model", "year");

        selectedData.show();

        selectedData.write()
                .option("header", "true")
                .csv("csvfile.csv");
        spark.stop();
    }
}
