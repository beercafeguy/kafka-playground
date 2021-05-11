package com.beercafeguy.kafka.rest;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
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

import java.util.Base64;

@SpringBootApplication
public class BinaryConsumerDemo implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(BinaryConsumerDemo.class);

    @Autowired
    RestTemplate restTemplate;

    public static void main(String[] args) {
        SpringApplication.run(BinaryConsumerDemo.class, args);
        // Step 1: Create a consumer group

        // Step 2: Subscribe to a topic (or list of topics)
        // Step 3: Get records
        // Step 4: Process records
        // Step 5: Commit Offset
    }


    @Override
    public void run(String... args) throws Exception {
        // Step 1: Create a consumer group
        log.info("Inside postBinaryData method");
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-type", "application/vnd.kafka.v2+json");

        HttpEntity<String> entity = new HttpEntity<>("{\n" +
                "  \"name\": \"my_consumer_binary\",\n" +
                "  \"format\": \"binary\",\n" +
                "  \"auto.offset.reset\": \"earliest\",\n" +
                "  \"auto.commit.enable\": \"false\"\n" +
                "}", headers);
        ResponseEntity<String> response = null;
        response = restTemplate.postForEntity("http://localhost:8082/consumers/rest_binary_cg", entity, String.class);
        log.info("Response Code:" + response.getStatusCode());
        log.info("Response Body:" + response.getBody());


        JSONParser parser = new JSONParser();
        Object obj = parser.parse(response.getBody());
        JSONObject jsonObject = (JSONObject) obj;
        String baseURI = (String) jsonObject.get("base_uri");
        log.info("Base URI:" + baseURI);

        // Step 2: Subscribe to a topic using base_uri(or list of topics)


        HttpEntity<String> subscribeEntity = new HttpEntity<>("{\n" +
                "  \"topics\": [\n" +
                "    \"rest-binary\"\n" +
                "  ]\n" +
                "}", headers);

        String subURI = baseURI + "/subscription";
        ResponseEntity<String> subscriptionResponse = restTemplate.postForEntity(subURI, subscribeEntity, String.class);
        log.info("Response Code:" + subscriptionResponse.getStatusCode());
        log.info("Response Body:" + subscriptionResponse.getBody());

        // Step 3: Get records

        HttpHeaders binaryHeaders = new HttpHeaders();
        binaryHeaders.set("Accept", "application/vnd.kafka.binary.v2+json");

        String readURI = baseURI + "/records?timeout=3000&max_bytes=300000";

        String offsetURI = baseURI + "/offsets";
        ResponseEntity<String> dataResponse
                = restTemplate.getForEntity(readURI, String.class);
        log.info("HTTP Status:" + dataResponse.getStatusCode());
        log.info("Response Body:" + dataResponse.getBody());



        //byte[] decodedBytes = Base64.getDecoder().decode(encodedBytes);
        //System.out.println("decodedBytes " + new String(decodedBytes));
        //System.exit(0);
    }

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder restTemplateBuilder) {
        return restTemplateBuilder.build();
    }
}
