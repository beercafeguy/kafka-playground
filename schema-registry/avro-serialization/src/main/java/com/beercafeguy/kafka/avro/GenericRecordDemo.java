package com.beercafeguy.kafka.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class GenericRecordDemo {

	public static void main(String args[]) throws IOException {
		// step 1: create schema
		Schema.Parser parser=new Schema.Parser();
		Schema schema=parser.parse(new File("src/main/resources/customer0.avsc"));
		//we can also pass the schema string
		//something like below but we need to escape double quotes so I prefer reading it from a file
		//parser.parse("{\"name\":\"Customer\"}");
		// step 2: create Generic Record object
		GenericRecordBuilder customerBuilder=new GenericRecordBuilder(schema);
		customerBuilder.set("first_name", "Hem");
		customerBuilder.set("last_name", "Chandra");
		customerBuilder.set("middle_name", "NA");
		customerBuilder.set("age", 26);
		customerBuilder.set("height", 170.2f);
		//test for non existing column
		//customerBuilder.set("country", "India");
		customerBuilder.set("weight", 204.1);
		customerBuilder.set("auto_email_turned_on", false);
		GenericRecord customer=customerBuilder.build();
		System.out.println("Create customer"+customer);
		// Step 3: Write Generic record to a file
		GenericRecord genericCustomer;
		final DatumWriter<GenericRecord> datumWriter=new GenericDatumWriter<GenericRecord>();
		try(DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)){
			dataFileWriter.create(customer.getSchema(),new File("customer.avro"));
			System.out.println("Write customer:"+customer);
			dataFileWriter.append(customer);
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		
		// Step 4: Read data from and avro file
		GenericRecord readCustomer;
		final DatumReader<GenericRecord> datumReader=new GenericDatumReader<GenericRecord>();
		try(DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File("customer.avro"),datumReader)){
			while(dataFileReader.hasNext()){
				readCustomer=dataFileReader.next();
				System.out.println("Read customer:"+readCustomer);
			}
			
		}
		
	}
}
