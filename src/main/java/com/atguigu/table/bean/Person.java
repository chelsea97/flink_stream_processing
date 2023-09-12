package com.atguigu.table.bean;

public class Person {
    private String id;
    private String name;
    private int age;
    private int score;
    private String cls;

    public Person(String id, String name, int age, String cls) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.score = score;
        this.cls = cls;
    }



    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    public int getScore() {
        return score;
    }

    public String getCls() {
        return cls;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public void setCls(String cls) {
        this.cls = cls;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", score=" + score +
                ", cls='" + cls + '\'' +
                '}';
    }
}
