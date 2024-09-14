
/**
 * @Description:
 * @author: Isam Al Jawarneh
 * @date: 12/03/2021*/
package com.unibo.kafkaProducerPack;

import com.unibo.beans.Nyc;
import com.csvreader.CsvReader;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.function.Supplier;


public class NYCsvReader implements Supplier<Nyc> {

    private final String filePath;
    private CsvReader csvReader;

    public NYCsvReader(String filePath) throws IOException {

        this.filePath = filePath;
        try {
            csvReader = new CsvReader(filePath);
            csvReader.readHeaders();
        } catch (IOException e) {
            throw new IOException("error reading data " + filePath, e);
        }
    }

    @Override
    public Nyc get() {
        Nyc userBehavior = null;
        try{
            if(csvReader.readRecord()) {
                csvReader.getRawRecord();
                userBehavior = new Nyc(



                       csvReader.get(0),
                        Double.valueOf(csvReader.get(1)),
                        Double.valueOf(csvReader.get(2)), Double.valueOf(csvReader.get(3))

                        //,new Date(Long.valueOf(csvReader.get(4))*1000L)
            );
            }
        } catch (IOException e) {
            throw new NoSuchElementException("IOException from " + filePath);
        }

        if (null==userBehavior) {
            throw new NoSuchElementException(" records  from " + filePath);
        }

        return userBehavior;
    }
}
