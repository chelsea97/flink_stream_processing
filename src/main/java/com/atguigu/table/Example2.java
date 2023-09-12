package com.atguigu.table;

import com.atguigu.table.bean.Person;
import org.apache.commons.collections.Factory;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.shaded.curator4.com.google.common.collect.Table;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

//batch processing scenario
public class Example2 {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<Person> personDataSource = env.readCsvFile("data/xxx.csv")
                .ignoreFirstLine()
                //pojoType is used to configure reader to read CSV data and parse it to the given type
                .pojoType(Person.class, "id", "name", "age", "score", "cls");
        //create table object based on current data
        //1. create table environment
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(env);
        tableEnvironment.registerDataSet("person",personDataSource);
        Table table = tableEnvironment.sqlQuery("select * from Table");
        DataSet<Row> dataSet = tableEnvironment.toDataSet(table, Row.class); //convert table to dataset based on table query result and class information
        dataSet.print();
    }
}
