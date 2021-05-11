package com.beercafeguy.kafka.rest.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.*;


@SpringBootApplication
public class RestProxyProducerDemo implements CommandLineRunner{
    private static final Logger log = LoggerFactory.getLogger(RestProxyProducerDemo.class);

    @Autowired
    RestTemplate restTemplate;

    public static void main(String[] args) {
        SpringApplication.run(RestProxyProducerDemo.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Starting REST client.");
        log.info("Get topic list from kafka rest proxy...");
        System.out.println(restTemplate.getForObject("http://localhost:8082/topics", List.class));
        HttpHeaders httpHeaders = restTemplate
                .headForHeaders("http://localhost:8082/topics");
        log.info("Header ContentType:"+httpHeaders.getContentType());
        log.info("Calling topic details method");
        //printTopicDetails();
        //postAvroData();
        postBinaryData();
        System.exit(0);
    }

    private void printTopicDetails() throws Exception{
        ArrayList topics=restTemplate.getForObject("http://localhost:8082/topics", ArrayList.class);
        log.info("Topics:"+topics);
        for (Object topic: topics) {
            log.info("Topic name:"+ (String)topic);
            String topicURL="http://localhost:8082/topics/"+(String)topic;
            //String topicDetails=restTemplate.getForObject(topicURL,String.class);
            ResponseEntity<String> response
                    = restTemplate.getForEntity(topicURL, String.class);
            log.info("HTTP Status:"+response.getStatusCode());
            log.info("Topic details for topic "+topic+" : "+response.getBody());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(response.getBody());
        }
    }

    private void postAvroData(){
        log.info("Inside postData method");
        HttpHeaders headers = new HttpHeaders();
        //headers.setContentType(new MediaType("application/vnd.kafka.avro.v2+json"));
        headers.set("Content-type", "application/vnd.kafka.avro.v2+json");
        headers.set("Accept", "application/vnd.kafka.v2+json");
        //List<MediaType> accepts=new ArrayList<MediaType>();
        //accepts.add(new MediaType("application/vnd.kafka.v2+json"));
        //accepts.add(new MediaType("application/vnd.kafka+json"));
        //accepts.add(new MediaType("application/json"));
        //headers.setAccept(accepts);

        HttpEntity<String> entity = new HttpEntity<>("{\n" +
                "  \"value_schema\": \"{\\\"type\\\": \\\"record\\\", \\\"name\\\": \\\"User\\\", \\\"fields\\\": [{\\\"name\\\": \\\"name\\\", \\\"type\\\": \\\"string\\\"}, {\\\"name\\\" :\\\"age\\\",  \\\"type\\\": [\\\"null\\\",\\\"int\\\"]}]}\",\n" +
                "  \"records\": [\n" +
                "    {\n" +
                "      \"value\": {\"name\": \"Hem Chandra\", \"age\": {\"int\": 25} }\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": {\"name\": \"Aman Chauhan\", \"age\": {\"int\": 28} },\n" +
                "      \"partition\": 0\n" +
                "    }\n" +
                "  ]\n" +
                "}", headers);
        ResponseEntity<String> response = restTemplate.postForEntity( "http://localhost:8082/topics/rest-avro", entity , String.class );
        log.info("Response:"+response.getStatusCode());

    }


    private void postBinaryData(){


        log.info("Inside postBinaryData method");
        Map<Integer,String> map=new HashMap<>();
        map.put(1,"Hem Chandra");
        map.put(2,"Ankur Das");
        map.put(3,"Gaurav Chauhan");
        map.put(4,"Mohita Rana");


        Map<String,String> base64Map=getMapBase64(map);

        JSONObject base64JsonObject = new JSONObject();
        JSONArray array=new JSONArray();
        for (Map.Entry entry:base64Map.entrySet()) {
            JSONObject obj = new JSONObject();
            obj.put("key",(String) entry.getKey());
            obj.put("value",(String) entry.getValue());
            array.add(obj);
        }
        base64JsonObject.put("records",array);
        log.info("Data:"+base64JsonObject.toJSONString());

        HttpHeaders headers = new HttpHeaders();
        //headers.setContentType(new MediaType("application/vnd.kafka.avro.v2+json"));
        headers.set("Content-type", "application/vnd.kafka.binary.v2+json");
        headers.set("Accept", "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json");

        HttpEntity<String> entity = new HttpEntity<>(base64JsonObject.toJSONString(),headers);
        /*HttpEntity<String> entity = new HttpEntity<>("{\n" +
                "  \"records\": [\n" +
                "    {\n" +
                "      \"key\": \"MTA=\",\n" +
                "      \"value\": \"S2FyYW4gQmFuc2Fs\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"TXVkaXQgS2F1bA==\",\n" +
                "      \"partition\": 0\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"UG9vamEgQmlzaHQ=\"\n" +
                "    }\n" +
                "  ]\n" +
                "}", headers);*/
        ResponseEntity<String> response = restTemplate.postForEntity( "http://localhost:8082/topics/rest-binary", entity , String.class );
        log.info("Response Code:"+response.getStatusCode());
        log.info("Response Body:"+response.getBody());

    }


    private void postJsonData(){

        log.info("Inside postJsonData method");
        HttpHeaders headers = new HttpHeaders();
        //headers.setContentType(new MediaType("application/vnd.kafka.avro.v2+json"));
        headers.set("Content-type", "application/vnd.kafka.binary.v2+json");
        headers.set("Accept", "application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json");
        //List<MediaType> accepts=new ArrayList<MediaType>();
        //accepts.add(new MediaType("application/vnd.kafka.v2+json"));
        //accepts.add(new MediaType("application/vnd.kafka+json"));
        //accepts.add(new MediaType("application/json"));
        //headers.setAccept(accepts);

        HttpEntity<String> entity = new HttpEntity<>("{\n" +
                "  \"records\": [\n" +
                "    {\n" +
                "      \"key\": \"MQ==\",\n" +
                "      \"value\": \"SGVtIENoYW5kcmE=\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"QW1hbiBDaGF1aGFu\",\n" +
                "      \"partition\": 0\n" +
                "    },\n" +
                "    {\n" +
                "      \"value\": \"QW5rdXIgRGFz\"\n" +
                "    }\n" +
                "  ]\n" +
                "}", headers);
        ResponseEntity<String> response = restTemplate.postForEntity( "http://localhost:8082/topics/rest-binary", entity , String.class );
        log.info("Response:"+response.getStatusCode());

    }

    private Map<String,String> getMapBase64(Map<Integer,String> map){
        Map<String,String> base64Map=new HashMap<>();
        for (Map.Entry entry:map.entrySet()){
            byte[] encodedKey = Base64.getEncoder().encode(entry.getKey().toString().getBytes());
            byte[] encodedValue = Base64.getEncoder().encode(((String)entry.getValue()).getBytes());
            base64Map.put(new String(encodedKey),new String(encodedValue));
        }
        return base64Map;
    }

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder restTemplateBuilder){
        return restTemplateBuilder.build();
    }


}
