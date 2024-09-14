
package com.unibo.kafkaProducerPack;

import com.unibo.beans.Shenzhen;
import com.csvreader.CsvReader;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * @Description:
 * @author: Isam Al Jawarneh
 * @date: 2021/04/2
 */
public class shenzhenCSVreader implements Supplier<Shenzhen> {

    private final String filePath;
    private CsvReader csvReader;

    public shenzhenCSVreader(String filePath) throws IOException {

        this.filePath = filePath;
        try {
            csvReader = new CsvReader(filePath);
            csvReader.readHeaders();
        } catch (IOException e) {
            throw new IOException("Error reading data: " + filePath, e);
        }
    }

    @Override
    public Shenzhen get() {
        Shenzhen shenzhen = null;
        try{
            if(csvReader.readRecord()) {
                csvReader.getRawRecord();
                shenzhen = new Shenzhen(



                        csvReader.get(0),
                        Double.valueOf(csvReader.get(1)),
                        Double.valueOf(csvReader.get(2)), csvReader.get(3),  Double.valueOf(csvReader.get(4))

                        //,new Date(Long.valueOf(csvReader.get(4))*1000L)
                );
            }
        } catch (IOException e) {
            throw new NoSuchElementException("IOException  " + filePath);
        }

        if (null==shenzhen) {
            throw new NoSuchElementException(" records  from " + filePath);
        }

        return shenzhen;
    }
}
