
package com.unibo.sede;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @Description: JSON Serializer
 * @author: Isam Al Jawarneh
 * @date: 2021/7/1
 */
public class JsonSerializer<T> {

    private final ObjectMapper jsonMapper = new ObjectMapper();

    public String toJSONString(T r) {
        try {
            return jsonMapper.writeValueAsString(r);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("not able to serialize record: " + r, e);
        }
    }

    public byte[] toJSONBytes(T r) {
        try {
            return jsonMapper.writeValueAsBytes(r);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("not able to serialize record: " + r, e);
        }
    }
}
