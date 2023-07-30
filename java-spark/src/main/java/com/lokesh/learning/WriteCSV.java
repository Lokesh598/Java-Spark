package com.lokesh.learning;
import com.lokesh.learning.processors.SaveToMongoDB;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.Serializable;

public class WriteCSV {
    public static void main(String[] args) {
        // Create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("CSVWriter")
                .master("local[*]")
                .getOrCreate();

        // Sample data with IDs as strings
        Dataset<Row> data = spark.createDataFrame(
                java.util.Arrays.asList(
                        new Car("ABC123", "Toyota", "Camry", 2019)
                ), Car.class
        );

        // Write data to a CSV file with IDs as strings
        data.write()
                .option("header", "true")
                .csv("file.csv");

        spark.stop();
    }

    public static class Car implements Serializable {
        private String licensePlate;
        private String make;
        private String model;
        private int year;

        public Car(String licensePlate, String make, String model, int year) {
            this.licensePlate = licensePlate;
            this.make = make;
            this.model = model;
            this.year = year;
        }

        public String getLicensePlate() {
            return licensePlate;
        }

        public void setLicensePlate(String licensePlate) {
            this.licensePlate = licensePlate;
        }

        public String getMake() {
            return make;
        }

        public void setMake(String make) {
            this.make = make;
        }

        public String getModel() {
            return model;
        }

        public void setModel(String model) {
            this.model = model;
        }

        public int getYear() {
            return year;
        }

        public void setYear(int year) {
            this.year = year;
        }
    }
}
