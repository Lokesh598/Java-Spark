package com.lokesh.learning.processors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;

import java.util.HashMap;
import java.util.concurrent.TimeoutException;

public class CsvToKafkaAndCsvProcessor {
    public static void main(String[] args) throws TimeoutException {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("CsvMonogoKafka")
                .getOrCreate();

        Dataset<Row> csvData = sparkSession.read()
                .format("csv")
                .option("header",true)
                .load("spark.car1.csv");

        csvData.show();

        Dataset<Row> mongoData = sparkSession.read()
                .format("com.mongodb.spark.sql.DefaultSource")
                .option("uri", "mongodb://localhost:27017/spark.car")
                .load();

        mongoData.printSchema();

        mongoData.show();

        /*Dataset<Row> joinedData = csvData.alias("csv").join(mongoData.alias("mongo"), csvData.col("_id").equalTo(mongoData.col("_id")));

        Dataset<Row> matchingRecords = joinedData.select("mongo._id", "mongo.licensePlate", "mongo.make", "mongo.model", "mongo.year");

        Dataset<Row> nonMatchingRecords = csvData.alias("csv")
                .join(mongoData.alias("mongo"), csvData.col("_id").equalTo(mongoData.col("_id")), "left_anti")
                .select("csv._id");*/

        //left outer join

        Dataset<Row> joinedData = csvData.alias("csv")
                .join(mongoData.alias("mongo"), csvData.col("_id").equalTo(mongoData.col("_id")), "left_outer");

        Dataset<Row> matchingRecords = joinedData
                .filter(joinedData.col("mongo._id").isNotNull())
                .select("mongo._id", "mongo.licensePlate", "mongo.make", "mongo.model", "mongo.year");

        Dataset<Row> nonMatchingRecords = joinedData
                .filter(joinedData.col("mongo._id").isNull())
                .select("csv._id");

        publishCsvRecordsToKafkaTopic(matchingRecords);

        nonMatchingRecords.write()
                .format("csv")
                .option("header", true)
                .save("car1.csv");

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
        kafkaConfigMap.put("topic", "car_record_2");
        return kafkaConfigMap;
    }
}
