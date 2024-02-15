/*
 * Copyright (C) 2023 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.flink.bigquery.sink.committer;

import org.apache.flink.api.connector.sink2.Committer;

import com.google.cloud.bigquery.storage.v1.FlushRowsRequest;
import com.google.cloud.bigquery.storage.v1.FlushRowsResponse;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.cloud.flink.bigquery.services.BigQueryServicesFactory;
import com.google.cloud.flink.bigquery.sink.committable.BigQueryCommittable;
import com.google.protobuf.Int64Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

/** */
public class BigQueryCommitter implements Committer<BigQueryCommittable>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryCommitter.class);

    private final boolean enableExactlyOnce;
    private final BigQueryConnectOptions connectOptions;

    public BigQueryCommitter(BigQueryConnectOptions connectOptions, boolean enableExactlyOnce) {
        this.connectOptions = connectOptions;
        this.enableExactlyOnce = enableExactlyOnce;
    }

    @Override
    public void commit(Collection<CommitRequest<BigQueryCommittable>> commitRequests)
            throws IOException, InterruptedException {
        if (!enableExactlyOnce) {
            LOG.info(
                    Thread.currentThread().getId()
                            + ": Called commit with no commit requests. Weird!");
            assert commitRequests.isEmpty();
            return;
        }
        assert commitRequests.size() == 1;
        BigQueryCommittable committable =
                (BigQueryCommittable)
                        ((CommitRequest) commitRequests.toArray()[0]).getCommittable();
        if (committable.getStreamOffset() == 0) {
            LOG.info(
                    Thread.currentThread().getId()
                            + ": Commit called for "
                            + committable.getStreamName()
                            + " with no records to flush. No Op!");
            return;
        }
        try (BigQueryServices.StorageWriteClient writeClient =
                BigQueryServicesFactory.instance(connectOptions).storageWrite()) {
            LOG.info(
                    Thread.currentThread().getId()
                            + ": Calling flush on stream "
                            + committable.getStreamName()
                            + " till offset "
                            + (committable.getStreamOffset() - 1));
            FlushRowsResponse response =
                    writeClient
                            .flushRows(
                                    FlushRowsRequest.newBuilder()
                                            .setOffset(
                                                    Int64Value.of(
                                                            committable.getStreamOffset() - 1))
                                            .setWriteStream(committable.getStreamName())
                                            .build())
                            .get();
            LOG.info(
                    Thread.currentThread().getId()
                            + ": Offset returned by flush on "
                            + committable.getStreamName()
                            + " is "
                            + response.getOffset());
        } catch (Exception e) {
            LOG.error(
                    Thread.currentThread().getId()
                            + ": FlushRows rpc failed for "
                            + committable.getStreamName(),
                    e);
            //            throw new RuntimeException(
            //                    Thread.currentThread().getId()
            //                            + ": Commit failed for "
            //                            + committable.getStreamName(),
            //                    e);
        }
//        try (BigQueryServices.StorageWriteClient writeClient =
//                BigQueryServicesFactory.instance(connectOptions).storageWrite()) {
//            LOG.info(
//                    Thread.currentThread().getId()
//                            + ": Calling second flush on stream "
//                            + committable.getStreamName()
//                            + " till offset "
//                            + (committable.getStreamOffset() - 2));
//            FlushRowsResponse response =
//                    writeClient
//                            .flushRows(
//                                    FlushRowsRequest.newBuilder()
//                                            .setOffset(
//                                                    Int64Value.of(
//                                                            committable.getStreamOffset() - 2))
//                                            .setWriteStream(committable.getStreamName())
//                                            .build())
//                            .get();
//            LOG.info(
//                    Thread.currentThread().getId()
//                            + ": Offset returned by second flush on "
//                            + committable.getStreamName()
//                            + " is "
//                            + response.getOffset());
//        } catch (Exception e) {
//            LOG.error(
//                    Thread.currentThread().getId()
//                            + ": Second FlushRows rpc failed for "
//                            + committable.getStreamName(),
//                    e);
//            //            throw new RuntimeException(
//            //                    Thread.currentThread().getId()
//            //                            + ": Commit failed for "
//            //                            + committable.getStreamName(),
//            //                    e);
//        }
//        try (BigQueryServices.StorageWriteClient writeClient =
//                BigQueryServicesFactory.instance(connectOptions).storageWrite()) {
//            LOG.info(
//                    Thread.currentThread().getId()
//                            + ": Calling third flush on stream "
//                            + committable.getStreamName()
//                            + " till offset "
//                            + (committable.getStreamOffset()));
//            FlushRowsResponse response =
//                    writeClient
//                            .flushRows(
//                                    FlushRowsRequest.newBuilder()
//                                            .setOffset(Int64Value.of(committable.getStreamOffset()))
//                                            .setWriteStream(committable.getStreamName())
//                                            .build())
//                            .get();
//            LOG.info(
//                    Thread.currentThread().getId()
//                            + ": Offset returned by third flush on "
//                            + committable.getStreamName()
//                            + " is "
//                            + response.getOffset());
//        } catch (Exception e) {
//            LOG.error(
//                    Thread.currentThread().getId()
//                            + ": Third FlushRows rpc failed for "
//                            + committable.getStreamName(),
//                    e);
//            //            throw new RuntimeException(
//            //                    Thread.currentThread().getId()
//            //                            + ": Commit failed for "
//            //                            + committable.getStreamName(),
//            //                    e);
//        }
    }

    @Override
    public void close() {
        LOG.info(Thread.currentThread().getId() + ": Committer closed");
    }
}
