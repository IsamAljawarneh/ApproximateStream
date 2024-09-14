

package com.unibo.sede;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;


public class JsonDeserializer<T> {

    private final Class<T> recordClazz;
    private final ObjectMapper jsonMapper;

    public JsonDeserializer(Class<T> recordClazz) {
        this.recordClazz = recordClazz;
        this.jsonMapper = new ObjectMapper();
    }

    public T parseFromString(String line) {
        try {
            return jsonMapper.readValue(line, this.recordClazz);
        } catch (IOException e) {
            throw new IllegalArgumentException("not able to  deserialize record: " + line + " as class " + recordClazz, e);
        }
    }
}
