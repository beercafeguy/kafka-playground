package com.beercafeguy.kafka.avro.specific;

import com.beercafeguy.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

public class SpecificRecordDemo {

    public static void main(String[] args) {

        // step 1: create specific record

        Customer.Builder builder = Customer.newBuilder();
        builder.setFirstName("Hem");
        builder.setLastName("Chandra");
        builder.setAge(30);
        builder.setHeight(139.4f);
        builder.setWeight(124.4);
        builder.setMiddleName("NA");
        builder.setAutoEmailTurnedOn(false);
        Customer customer = builder.build();
        System.out.println(customer);
        // step 2: write specific record
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        try (DataFileWriter<Customer> writer = new DataFileWriter<Customer>(datumWriter)) {

            writer.create(customer.getSchema(),new java.io.File("specific_customer.avro"));
            writer.append(customer);
            System.out.println("File: specific_customer.avro written successfully.");
        } catch (Exception ex) {

           ex.printStackTrace();
        }
        // step 3: read specific record


        final java.io.File file=new java.io.File("specific_customer.avro");
        final DatumReader<Customer> datumReader=new SpecificDatumReader<>(Customer.class);
        try(DataFileReader<Customer> dataFileReader=new DataFileReader<Customer>(file,datumReader)){
            System.out.println("Reading specific record....");
            while(dataFileReader.hasNext()){
                Customer readCustomer=dataFileReader.next();
                System.out.println("Read customer:"+customer);
                System.out.println("First name:"+customer.getFirstName());
            }

        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

}
