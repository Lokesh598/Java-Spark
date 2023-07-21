package com.lokesh.learning.model.entity;

import java.io.Serializable;

public class Car implements Serializable {
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
}
