package com.beercafeguy.kafka.rest.producer;

public class KeyValueClass {
    private String key;
    private String value;

    public KeyValueClass(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
