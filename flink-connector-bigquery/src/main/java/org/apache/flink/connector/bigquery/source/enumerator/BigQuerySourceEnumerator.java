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

package org.apache.flink.connector.bigquery.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.bigquery.source.split.BigQuerySourceSplit;
import org.apache.flink.connector.bigquery.source.split.BigQuerySourceSplitAssigner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

/** The enumerator class for {@link BigQuerySource}. */
@Internal
public class BigQuerySourceEnumerator
        implements SplitEnumerator<BigQuerySourceSplit, BigQuerySourceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySourceSplitAssigner.class);

    private final Boundedness boundedness;
    private final SplitEnumeratorContext<BigQuerySourceSplit> context;
    private final BigQuerySourceSplitAssigner splitAssigner;
    private final TreeSet<Integer> readersAwaitingSplit;

    public BigQuerySourceEnumerator(
            Boundedness boundedness,
            SplitEnumeratorContext<BigQuerySourceSplit> context,
            BigQuerySourceSplitAssigner splitAssigner) {
        this.boundedness = boundedness;
        this.context = context;
        this.splitAssigner = splitAssigner;
        this.readersAwaitingSplit = new TreeSet<>();
    }

    @Override
    public void start() {
        
        
        splitAssigner.open();
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        readersAwaitingSplit.add(subtaskId);
        assignSplits();
    }

    @Override
    public void addSplitsBack(List<BigQuerySourceSplit> splits, int subtaskId) {
        LOG.debug("BigQuery Source Enumerator adds splits back: {}", splits);
        splitAssigner.addSplitsBack(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        LOG.debug("Adding reader {} to BigQuerySourceEnumerator.", subtaskId);
    }

    @Override
    public BigQuerySourceEnumState snapshotState(long checkpointId) throws Exception {
        BigQuerySourceEnumState state = splitAssigner.snapshotState(checkpointId);
        LOG.debug("Checkpointing state {}", state);
        return state;
    }

    @Override
    public void close() throws IOException {
        splitAssigner.close();
    }

    private void assignSplits() {
        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();

        while (awaitingReader.hasNext()) {
            int nextAwaiting = awaitingReader.next();
            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().containsKey(nextAwaiting)) {
                awaitingReader.remove();
                continue;
            }

            Optional<BigQuerySourceSplit> split = splitAssigner.getNext();
            if (split.isPresent()) {
                final BigQuerySourceSplit bqSplit = split.get();
                context.assignSplit(bqSplit, nextAwaiting);
                awaitingReader.remove();
                LOG.info("Assign split {} to subtask {}", bqSplit, nextAwaiting);
                break;
            } else if (splitAssigner.noMoreSplits() && boundedness == Boundedness.BOUNDED) {
                LOG.info("All splits have been assigned");
                context.registeredReaders().keySet().forEach(context::signalNoMoreSplits);
                break;
            } else {
                // there is no available splits by now, skip assigning
                break;
            }
        }
    }
}
