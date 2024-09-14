

/*
 * Copyright 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.unibo.beans;

        import com.fasterxml.jackson.annotation.JsonFormat;

//import java.util.Date;

/**
 * @Description: Shenzhen JSON Object
 * @author: Isam Al Jawarneh
 * @date: 2021/7/2
 */
public class Shenzhen {


    @JsonFormat
    private String id;

    @JsonFormat
    private double lat;

    @JsonFormat
    private double lon;

    @JsonFormat
    private String time;

    @JsonFormat
    private Double speed;

    //@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    //private Date ts;

    public String getDriver_id(){return id;}
    public Shenzhen() {
    }

    public Shenzhen(String id, double lat, double lon,String time,double speed) {
        this.id = id;
        this.lat = lat;
        this.lon = lon;
        this.time = time;
        this.speed = speed;


    }
}
