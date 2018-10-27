package com.calabar.flinkDemo.hbase.loader;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;


public class HBaseLoader implements ILoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseLoader.class);
    @Override
    public void loader() throws Exception {

        Table table = null;
        try {
            Configuration conf = HBaseConfiguration.create();
            Connection conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf("dfdq_rhm_aly:f_aly_point_data_test"));
            Put put = new Put("kkk".getBytes());
            put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("t"),Bytes.toBytes(System.currentTimeMillis()));
            table.put(put);
        } catch (Exception e) {
            throw new Exception("批量存储数据失败！", e);
        } finally {
//            table.close();
        }
    }

    public static void main(String[] args) throws Exception {
        ILoader loader = new HBaseLoader();
        loader.loader();
    }


    public void loadSpeed(Table table, String family, String value) {
        long start = System.currentTimeMillis();
        byte[] fam_b = Bytes.toBytes(family);
        byte[] slv_b = Bytes.toBytes("slv");

        // 装入多行数据
        List<Put> puts = new LinkedList<>();
        Put put;
        //for (VibSaveEntry aData : data) {
        put = new Put(Bytes.toBytes("lastSpeed"+value));
        put.addColumn(fam_b, slv_b, Bytes.toBytes(value));
        puts.add(put);
        //}
        if (CollectionUtils.isNotEmpty(puts)) {
            try {
                HBaseRetryingUtils.retrying(table, puts);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long end = System.currentTimeMillis();
        LOGGER.debug("数据存储耗时："+(end-start));
    }
}
