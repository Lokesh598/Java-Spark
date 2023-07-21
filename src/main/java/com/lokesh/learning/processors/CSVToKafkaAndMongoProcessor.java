package com.lokesh.learning.processors;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.storage.StorageLevel;

import java.util.HashMap;
import java.util.concurrent.TimeoutException;

/**
 * @Task:1. read csv
 * 2. read collection
 * 3. matched data publish in kafka and unmached data into error collection
 */

public class CSVToKafkaAndMongoProcessor {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("CsvMonogoKafka")
                .getOrCreate();

        Dataset<Row> csvData = sparkSession.read()
                .format("csv")
                .option("header",true)
                .load("spark.car1.csv");

        csvData = csvData.withColumn("_id", org.apache.spark.sql.functions.struct(csvData.col("_id").alias("oid")));

        csvData.show();

        Dataset<Row> mongoData = sparkSession.read()
                .format("com.mongodb.spark.sql.DefaultSource")
                        .option("uri", "mongodb://localhost:27017/spark.car")
                                .load();

        mongoData.printSchema();

        mongoData.show();

        Dataset<Row> joinedData = csvData.alias("csv").join(mongoData.alias("mongo"), csvData.col("_id.oid").equalTo(mongoData.col("_id.oid")));

        // Split the joined DataFrame into two DataFrames
        Dataset<Row> matchingRecords = joinedData.select("mongo._id", "mongo.licensePlate", "mongo.make", "mongo.model", "mongo.year");

//        Dataset<Row> nonMatchingRecords = csvData.alias("csv")
//                .join(mongoData.alias("mongo"), csvData.col("_id.oid").equalTo(mongoData.col("_id.oid")), "left_anti")
//                .select("csv._id", "csv.licensePlate", "csv.make", "csv.model", "csv.year");

        Dataset<Row> nonMatchingRecords1 = mongoData.alias("mongo")
                        .join(csvData.alias("csv"), mongoData.col("_id.oid").equalTo(csvData.col("_id.oid")), "left_anti")
                                .select("mongo._id", "mongo.licensePlate", "mongo.make", "mongo.model", "mongo.year");

        matchingRecords.show();
//        nonMatchingRecords.show();
        nonMatchingRecords1.show();

//        nonMatchingRecords.write()
//                .format("mongo")
//                .option("uri", "mongodb://localhost:27017/spark.error_collection_5")
//                .save();

        nonMatchingRecords1.write()
                .format("mongo")
                .option("uri", "mongodb://localhost:27017/spark.error_collection_6")
                .save();

        publishCsvRecordsToKafkaTopic(matchingRecords);

        sparkSession.stop();
    }
    private static void publishCsvRecordsToKafkaTopic(Dataset<Row> rowDataset) throws TimeoutException {
        if (null != rowDataset) {
            Dataset<Row> KafkaPublishJson = rowDataset
                    .withColumn("value", functions.to_json(functions.struct(functions.col("_id"),
                            functions.col("licensePlate"), functions.col("make"),
                            functions.col("model"), functions.col("year"))))
                    .alias("value").select("value")
                    .persist(StorageLevel.MEMORY_AND_DISK());
            KafkaPublishJson
                    .write().format("kafka").options(KafkaCsvConfig()).save();
        }
    }

    private static java.util.Map<String,String> KafkaCsvConfig(){
        java.util.Map<String, String> kafkaConfigMap = new HashMap<>();
        kafkaConfigMap.put("kafka.bootstrap.servers","127.0.0.1:9092");
        kafkaConfigMap.put("topic", "car_record_test_6");
        return kafkaConfigMap;
    }
}

