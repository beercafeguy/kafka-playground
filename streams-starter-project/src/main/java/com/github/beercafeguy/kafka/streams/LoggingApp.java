package com.github.beercafeguy.kafka.streams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingApp {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(LoggingApp.class);
        logger.info("THis is how you log info");
    }
}
