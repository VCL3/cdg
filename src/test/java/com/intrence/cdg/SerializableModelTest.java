package com.intrence.cdg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests serialization and deserialization automatically for any class that implements
 */
abstract public class SerializableModelTest<T> {
    protected static final ObjectMapper JSON_MAPPER = (new ObjectMapper()).registerModule(new JodaModule());

    protected abstract T getTestInstance();

    protected abstract Class<T> getType();

    @Test
    public void testShouldSerializeAndDeserialize() throws Exception {
        T model = getTestInstance();
        String json = JSON_MAPPER.writeValueAsString(model);
        T deserialized = JSON_MAPPER.readValue(json, getType());
        assertEquals(model, deserialized);
    }

    @Test
    public void testShouldBeBackwardsCompatibleWithNewFields() throws Exception {
        T model = getTestInstance();
        String json = JSON_MAPPER.writeValueAsString(model);
        // super ghetto add new field
        json = json.replaceFirst("\\{", "{\"foo\":\"bar\",");
        T deserialized = JSON_MAPPER.readValue(json, getType());
        assertEquals(model, deserialized);
    }

}
