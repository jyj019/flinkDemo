package com.calabar.flinkDemo.hbase;


import com.calabar.flinkDemo.hbase.loader.HBaseLoader;
import com.calabar.flinkDemo.hbase.loader.HBaseUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * <p/>
 * <li>@author: jyj019 </li>
 * <li>Date: 2018/9/18 10:07</li>
 * <li>@version: 2.0.0 </li>
 * <li>@since JDK 1.8 </li>
 */
public class Flink2HBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(Flink2HBase.class);

    public static void main(String[] args) {

        System.setProperty("hadoop.home.dir", "C:\\hbase-1.1.2");
        System.setProperty("HADOOP_USER_NAME", "hdfs");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000); // 非常关键，一定要设置启动检查点！！
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        DataStream<String> transction = env.addSource(new FlinkKafkaConsumer010<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties()));
        //DataStream<String> transction1 = env.addSource(new FlinkKafkaConsumer010<String>("test3",new SimpleStringSchema(), props));

        //DataStream<Event> eventDataStream=transction.map((line)->parse(line));

        transction.rebalance().map(new MapFunction<String, Object>() {

            public String map(String value)throws IOException {

                writeIntoHBase(value);
                return value;
            }

        });


        transction.rebalance().map(new MapFunction<String, Object>() {

            @Override
            public String map(String value)throws IOException {

                writeIntoHBase(value);
                return value;
            }

        });

        //transction.writeAsText("/home/admin/log2");
        // transction.addSink(new HBaseOutputFormat();
        try {
            env.execute();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }




    public static void writeIntoHBase(String value)throws IOException {
        HBaseLoader hBaseLoader= new HBaseLoader();
        String hBaseTable="dfdq_rhm_aly:f_turbine_event_data";
        String hBaseTableCF="f";
        Table table = null;
        // 常量

//        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
//        config.set("hbase.zookeeper.property.clientPort", "2181");
//
//        config.set("hbase.zookeeper.quorum", "bigdata-master2.phmcluster.calabar,bigdata-master1.phmcluster.calabar,bigdata-slave1.phmcluster.calabar,bigdata-slave2.phmcluster.calabar,bigdata-slave3.phmcluster.calabar");
//        config.set("zookeeper.znode.parent", "/hbase-unsecure");

        //config.set(TableOutputFormat.OUTPUT_TABLE, hbasetable);

        try {
            table = HBaseUtils.getConnection().getTable(TableName.valueOf(hBaseTable));
        } catch (Exception e) {
            LOGGER.error("HBase连接建立出错",e);
            e.printStackTrace();
        }

        hBaseLoader.loadSpeed(table, hBaseTableCF,String.valueOf(value));

//        Connection c = ConnectionFactory.createConnection(config);
//
//        Admin admin = c.getAdmin();
//        if(!admin.tableExists(tableName)){
//            admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(columnFamily)));
//        }
//        Table t = c.getTable(tableName);
//
//        TimeStamp ts = new TimeStamp(new Date());
//
//        Date date = ts.getDate();
//
//        Put put = new Put(org.apache.hadoop.hbase.util.Bytes.toBytes(date.toString()));
//
//        put.addColumn(org.apache.hadoop.hbase.util.Bytes.toBytes(columnFamily), org.apache.hadoop.hbase.util.Bytes.toBytes("test"),
//                org.apache.hadoop.hbase.util.Bytes.toBytes(m));
//        t.put(put);
//
//        t.close();
//        c.close();
    }




}
