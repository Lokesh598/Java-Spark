package com.lokesh.learning.processors;

import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class ReadCsvPublishToKafka {
    public static void main(String[] args) throws TimeoutException {
        SparkSession spark = SparkSession.builder()
                .appName("CSVToKafkaProcessor")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> csvData = spark.read()
                .format("csv")
                .option("header", "true")
                .load("spark.car.csv");

        csvData.show();
        publishCsvRecordsToKafkaTopic(csvData);
        spark.stop();
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
        Map<String, String> kafkaConfigMap = new HashMap<>();
        kafkaConfigMap.put("kafka.bootstrap.servers","127.0.0.1:9092");
        kafkaConfigMap.put("topic", "car_matching_records_topic");
        return kafkaConfigMap;
    }
}

