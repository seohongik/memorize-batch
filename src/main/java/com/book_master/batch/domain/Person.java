package com.book_master.batch.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import java.util.Objects;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Person {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;
    private String name;
    private String age;
    private String address;

    public Person(String name, String age, String address) {
        this.address = address;
        this.name=name;
        this.age =age;
    }

    public boolean isNotEmptyName() {
        return Objects.nonNull(this.name) && !name.isEmpty();
    }

    public Person unknownName() {
        this.name ="UNKNOWN";
        return this;
    }
}
