package com.calabar.flinkDemo.hbase.loader;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p/>
 * <li>@author: jinyujie <yujie.jin@cdcalabar.com> </li>
 * <li>Date: 2018/6/12 9:35</li>
 * <li>@version: 2.0.0 </li>
 * <li>@since JDK 1.8 </li>
 */
public class HBaseRetryingUtils implements Serializable {
    /**
     * 日志记录
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseRetryingUtils.class);

    /**
     * 重试发送数据到hbase
     *
     * @param table
     * @param puts      List<Put>
     * @throws Exception 连接异常
     */
    public static void retrying(Table table, List<Put> puts) throws Exception {
        // 异常或者返回null都继续重试、每3秒重试一次、最多重试5次
        Retryer<Boolean> retryer = RetryerBuilder.<Boolean>newBuilder()
                .retryIfException()
                .withWaitStrategy(WaitStrategies.fixedWait(500, TimeUnit.MILLISECONDS))
                .withStopStrategy(StopStrategies.stopAfterAttempt(6))
                .build();

        try {
            retryer.call(() -> HBaseUtils.batchPuts(table, puts));
        } catch (Exception e) {
            LOGGER.error("多次重试发送数据到hbase失败！", e);
            throw new Exception("多次重试发送数据到hbase失败！", e);
        }
    }
}
