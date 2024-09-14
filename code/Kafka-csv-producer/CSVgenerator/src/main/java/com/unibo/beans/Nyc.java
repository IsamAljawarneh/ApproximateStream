
package com.unibo.beans;

import com.fasterxml.jackson.annotation.JsonFormat;

/**
 *
 * @author: Isam Al Jawarneh
 * @date: 2021/1/1
 */
public class Nyc {


    @JsonFormat
    private String vendorId;

    @JsonFormat
    private double Pickup_longitude;

    @JsonFormat
    private double Pickup_latitude;

    @JsonFormat
    private double Trip_distance;

    //@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    //private Date ts;

public String getDriver_id(){return vendorId;}
    public Nyc() {
    }

    public Nyc(String vendorId, double Pickup_longitude, double Pickup_latitude, double Trip_distance) {
        this.vendorId = vendorId;
        this.Pickup_longitude = Pickup_longitude;
        this.Pickup_latitude = Pickup_latitude;
        this.Trip_distance = Trip_distance;

    }
}
