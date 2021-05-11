package com.beercafeguy.avro.evolution;

import com.beercafeguy.CustomerV1;
import com.beercafeguy.CustomerV2;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class BackwordCompatibleDemo {

    public static void main(String[] args) {
        //creating customer v1 object
        CustomerV1 customerV1= CustomerV1.newBuilder()
                .setAge(28)
                .setFirstName("Hem")
                .setLastName("Chandra")
                .setHeight(165.0f)
                .setWeight(203.4)
                .setAutoEmailTurnedOn(true)
                .build();
        System.out.println("Customer V1:"+customerV1);

        //Write the customer onject to avro file using customerV1 schema
        final java.io.File file=new java.io.File("customer-v1.avro");
        final DatumWriter<CustomerV1> datumWriter=new SpecificDatumWriter<CustomerV1>(CustomerV1.class);
        try(DataFileWriter<CustomerV1> writer=new DataFileWriter<CustomerV1>(datumWriter)){

            writer.create(customerV1.getSchema(),file);
            writer.append(customerV1);
            System.out.println("Customer V1"+customerV1+ " is successfully written to file.");
        }catch(Exception ex){
            ex.printStackTrace();
        }



        //Read the customer onject from avro file using customerV2 schema
        final DatumReader<CustomerV2> datumReader=new SpecificDatumReader<CustomerV2>(CustomerV2.class);
        try(DataFileReader<CustomerV2> reader=new DataFileReader<CustomerV2>(file,datumReader)){
            while(reader.hasNext()) {

                CustomerV2 customerV2=reader.next();
                System.out.println("Customer V2"+customerV2+ " is successfully read from file.");
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
