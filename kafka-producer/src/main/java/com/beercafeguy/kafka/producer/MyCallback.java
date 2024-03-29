package com.beercafeguy.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyCallback implements Callback {

    private Integer messageKey;
    private String message;
    private Long startTime;

    public MyCallback(Integer messageKey, String message, Long startTime) {
        this.messageKey = messageKey;
        this.message = message;
        this.startTime = startTime;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        long elapsedTime=System.currentTimeMillis()-startTime;
        if(null!=recordMetadata){
            System.out.println("message(" + messageKey + ", " + message + ") sent to partition(" + recordMetadata.partition() +
                    "), " +
                    "offset(" + recordMetadata.offset() + ") in " + elapsedTime + " ms");
        }else{
            e.printStackTrace();
        }

    }
}
