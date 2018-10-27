package com.calabar.flinkDemo.hbase;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * <p/>
 * <li>@author: jyj019 </li>
 * <li>Date: 2018/9/18 14:31</li>
 * <li>@version: 2.0.0 </li>
 * <li>@since JDK 1.8 </li>
 */
public class FlinkGHBaseByDataSet {

    public static void main(String[] args) {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<String, String>> hbaseInput =  env.createInput(new TableInputFormat<Tuple2<String, String>>(){
            @Override
            protected Scan getScanner() {
                Scan scan = new Scan();
                scan.setStartRow(Bytes.toBytes("lastSpeed1"));
                scan.setStopRow(Bytes.toBytes("lastSpeed4"));
                return scan;
            }
            @Override
            protected String getTableName() {
                return "dfdq_rhm_aly:f_turbine_event_data";
            }
            @Override
            protected Tuple2<String, String> mapResultToTuple(Result result) {

                Tuple2<String,String> tup = new Tuple2<String,String>();
                tup.setField(Bytes.toString(result.getRow()),0);
                tup.setField(Bytes.toString(result.getValue("f".getBytes(), "slv".getBytes())), 1);
                return tup;
            }
        });

        try {
            hbaseInput.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
