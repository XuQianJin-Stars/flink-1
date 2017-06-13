package com.xiuhong.flink.dataartisans.datastreamapi.one;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * author:xiuhong
 * date:6/9/17
 * time:09:20:06
 */
public class Transformations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> p = env.fromElements(new Person(1, "Bob"));
        p.keyBy("id");

        p.print();
        env.execute();

    }
}

class Person {
    public int id;
    public String name;

    public Person() {
    }

    ;

    public Person(int id, String name) {
        this.id = id;
        this.name = name;

    }

    ;
}