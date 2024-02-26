package com.google.cloud.flink.bigquery.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration;
import org.apache.flink.connector.base.DeliveryGuarantee;

import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;

/** */
public class BigQuerySinkConfigurations {

    final int parallelism;
    final int maxParallelism;
    final BigQueryConnectOptions connectOptions;
    final DeliveryGuarantee deliveryGuarantee;
    final RestartStrategyConfiguration restartStrategy;

    public BigQuerySinkConfigurations(
            int parallelism,
            int maxParallelism,
            BigQueryConnectOptions connectOptions,
            DeliveryGuarantee deliveryGuarantee,
            RestartStrategyConfiguration restartStrategy) {
        this.parallelism = parallelism;
        this.maxParallelism = maxParallelism;
        this.connectOptions = connectOptions;
        this.deliveryGuarantee = deliveryGuarantee;
        this.restartStrategy = restartStrategy;
    }
}
