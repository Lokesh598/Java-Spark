package com.lokesh.learning.processors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.lokesh.learning.model.entity.Car;

import java.io.Serializable;

public class SaveToMongoDB {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SaveToMongoDB")
                .master("local[*]")
                .getOrCreate();

        Car car1 = new Car("ABC123", "Toyota", "Camry", 2019);
        Car car2 = new Car("XYZ456", "Honda", "Civic", 2020);

        // Convert Car objects to a DataFrame
        Dataset<Row> carDF = spark.createDataFrame(
                java.util.Arrays.asList(car1, car2), Car.class
        );

        carDF.write()
                .format("mongo")
                .option("uri", "mongodb://localhost:27017/spark.car-data")
                .save();

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
