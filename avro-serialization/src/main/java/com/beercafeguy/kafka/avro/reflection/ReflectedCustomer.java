package com.beercafeguy.kafka.avro.reflection;

import org.apache.avro.reflect.Nullable;

public class ReflectedCustomer {

    private String firstName;
    private String lastName;
    @Nullable private int age;

    public ReflectedCustomer() {
    }

    public ReflectedCustomer(String firstName, String lastName, int age) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public int getAge() {
        return age;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getFullName(){
        return firstName+" "+lastName;
    }

    @Override
    public String toString() {
        return "ReflectedCustomer{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", age=" + age +
                '}';
    }
}
