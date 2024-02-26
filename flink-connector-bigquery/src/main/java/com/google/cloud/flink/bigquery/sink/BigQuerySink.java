package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;

/** */
public class BigQuerySink {

    public static void addBigQuerySink(
            BigQuerySinkConfigurations sinkConfig,
            StreamExecutionEnvironment env,
            DataStream dataStream) {
        Sink bqSink =
                createSink(
                        sinkConfig.connectOptions,
                        sinkConfig.deliveryGuarantee,
                        sinkConfig.maxParallelism);
        // validate restart strategy
        env.setRestartStrategy(sinkConfig.restartStrategy);
        if (sinkConfig.maxParallelism > 10000
                || sinkConfig.parallelism > sinkConfig.maxParallelism) {
            throw new IllegalArgumentException("Invalid sink parallelism configured");
        }
        dataStream.rebalance().sinkTo(bqSink).setParallelism(sinkConfig.parallelism);
    }

    private static Sink createSink(
            BigQueryConnectOptions connectOptions,
            DeliveryGuarantee deliveryGuarantee,
            int maxParallelism) {
        if (deliveryGuarantee.compareTo(DeliveryGuarantee.EXACTLY_ONCE) == 0) {
            return new ExactlyOnceSink(connectOptions, maxParallelism);
        }
        return new DefaultSink(connectOptions, maxParallelism);
    }
}
