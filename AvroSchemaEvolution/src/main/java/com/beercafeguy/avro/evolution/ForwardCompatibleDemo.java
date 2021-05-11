package com.beercafeguy.avro.evolution;

import com.beercafeguy.CustomerV1;
import com.beercafeguy.CustomerV2;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class ForwardCompatibleDemo {
    public static void main(String[] args) {

        //creating customer v1 object
        CustomerV2 customerV2= CustomerV2.newBuilder()
                .setAge(28)
                .setFirstName("Hem")
                .setLastName("Thuwal")
                .setHeight(165.0f)
                .setWeight(203.4)
                .setAutoEmailTurnedOn(true)
                .setMiddleName("Chandra")
                .build();
        System.out.println("Customer V2:"+customerV2);

        //Write the customer onject to avro file using customerV2 schema
        final java.io.File file=new java.io.File("customer-v2.avro");
        final DatumWriter<CustomerV2> datumWriter=new SpecificDatumWriter<CustomerV2>(CustomerV2.class);
        try(DataFileWriter<CustomerV2> writer=new DataFileWriter<CustomerV2>(datumWriter)){

            writer.create(customerV2.getSchema(),file);
            writer.append(customerV2);
            System.out.println("Customer V2"+customerV2+ " is successfully written to file.");
        }catch(Exception ex){
            ex.printStackTrace();
        }



        //Read the customer onject from avro file using customerV1 schema
        final DatumReader<CustomerV1> datumReader=new SpecificDatumReader<CustomerV1>(CustomerV1.class);
        try(DataFileReader<CustomerV1> reader=new DataFileReader<CustomerV1>(file,datumReader)){
            while(reader.hasNext()) {
                CustomerV1 customerV1=reader.next();
                System.out.println("Customer V1"+customerV1+ " is successfully read from file.");
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
