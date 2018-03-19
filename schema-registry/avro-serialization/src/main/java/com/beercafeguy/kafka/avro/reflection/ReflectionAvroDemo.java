package com.beercafeguy.kafka.avro.reflection;

import org.apache.avro.Schema;
import java.io.*;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

public class ReflectionAvroDemo {

    public static void main(String[] args) {

        // step 1: Create schema using reflection
        Schema schema= ReflectData.get().getSchema(ReflectedCustomer.class);

        // step 2: Write data to avro file
        System.out.println("Writing reflected-customer.avro");
        File file=new File("reflected-customer.avro");
        DatumWriter<ReflectedCustomer> datumWriter=new ReflectDatumWriter<>(ReflectedCustomer.class);
        try(DataFileWriter<ReflectedCustomer> writer=new DataFileWriter<>(datumWriter).setCodec(CodecFactory.deflateCodec(9))){
            writer.create(schema,file);
            writer.append(new ReflectedCustomer("Hem","Chandra",28));
            System.out.println("Data written to reflected-customer.avro successfully");
        }catch(Exception ex){
            ex.printStackTrace();
        }


        //step 3 : read data from avro file
        System.out.println("Reading reflected-customer.avro");
        File readFile=new File("reflected-customer.avro");
        DatumReader<ReflectedCustomer> datumReader=new ReflectDatumReader<>(ReflectedCustomer.class);

        try(DataFileReader<ReflectedCustomer> reader=new DataFileReader<>(readFile,datumReader)){
            while(reader.hasNext()) {
                ReflectedCustomer readCustomer = reader.next();
                System.out.println("Customer from file:"+readCustomer);
            }

            System.out.println("File read completed");
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
