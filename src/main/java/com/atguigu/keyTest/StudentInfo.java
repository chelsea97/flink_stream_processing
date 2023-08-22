package com.atguigu.keyTest;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

public class StudentInfo {
    private String name;
    private String gender;
    private Integer age;
    private Tuple2<String,Tuple2<Integer,Integer>> classAndScore;
    public StudentInfo() {

    }
    public StudentInfo(String name, String gender, Integer age, Tuple2<String, Tuple2<Integer,Integer>> classAndScore ){
        this.name = name;
        this.gender = gender;
        this.age = age;
        this.classAndScore = classAndScore;
    }
    @Override
    public int hashCode(){
        return super.hashCode();
    }
    @Override
    public String toString(){
        return "StudentInfo{"+
                "name='"+name+'\''+
                ",gender='"+gender+'\''+
                ",age="+age+
                ",classAndScore="+classAndScore+
                '}';
    }

    public String getName() {
        return this.name;
    }
    public String getGender(){
        return this.gender;
    }
    public Integer getAge(){
        return this.age;
    }
    public Tuple2<String,Tuple2<Integer,Integer>> getClassAndScore(){
        return this.classAndScore;
    }
}
