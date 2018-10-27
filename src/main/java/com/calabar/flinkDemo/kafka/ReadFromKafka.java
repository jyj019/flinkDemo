package com.calabar.flinkDemo.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.HashMap;
import java.util.Map;


/**
 * <p/>
 * <li>@author:jyj019 </li>
 * <li>Date: 2018/9/17 14:50</li>
 * <li>@version: 2.0.0 </li>
 * <li>@since JDK 1.8 </li>
 */


public class ReadFromKafka {

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Map properties= new HashMap();
        properties.put("bootstrap.servers", "192.168.10.63:6667,192.168.10.64:6667,192.168.10.65:6667");
        properties.put("group.id", "dec-esc-group-vib-calc");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("topic", "dec-vibration-test");
        //KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        // parse user parameters
        //ParameterTool parameterTool = ParameterTool.fromArgs(args);
        ParameterTool parameterTool = ParameterTool.fromMap(properties);

        FlinkKafkaConsumer010 consumer010 = new FlinkKafkaConsumer010(
                         parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties());

      //  consumer010.setStartFromEarliest();

        DataStream<String> messageStream = env
                .addSource(consumer010);

        // print() will write the contents of the stream to the TaskManager's standard out stream
        // the rebelance call is causing a repartitioning of the data so that all machines
        // see the messages (for example in cases when "num kafka partitions" < "num flink operators"
        messageStream.rebalance().map(new MapFunction<String, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public String map(String value) throws Exception {
                return value;

            }
        });


        messageStream.print();

        env.execute();
    }
}