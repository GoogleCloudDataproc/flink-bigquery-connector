/*
 * Copyright (C) 2024 Google Inc.
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

package com.google.cloud.flink.bigquery.sink.indirect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Tracks completion state for a single checkpoint's committable messages. */
@Internal
final class CheckpointState {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointState.class);

    private final Set<CommittableSummary<FileSinkCommittable>> summaries = new HashSet<>();
    private final Set<CommittableWithLineage<FileSinkCommittable>> committables = new HashSet<>();

    /**
     * Adds a committable message to this checkpoint's state.
     *
     * <p>{@link
     * org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology#addPostCommitTopology}
     * permits any committable to be replayed on recovery, so summaries and committables are stored
     * in deduping collections rather than tracked with running counts. {@link #isComplete()}
     * recomputes totals on each call, making duplicates a no-op.
     */
    void add(final CommittableMessage<FileSinkCommittable> message) {
        if (message instanceof CommittableSummary) {
            final CommittableSummary<FileSinkCommittable> summary =
                    (CommittableSummary<FileSinkCommittable>) message;
            summaries.add(summary);
            LOG.debug(
                    "Received CommittableSummary for checkpoint {}: subtaskId={}, "
                            + "numberOfSubtasks={}, numberOfCommittables={}, summariesReceived={}",
                    summary.getCheckpointId(),
                    summary.getSubtaskId(),
                    summary.getNumberOfSubtasks(),
                    summary.getNumberOfCommittables(),
                    summaries.size());
        } else if (message instanceof CommittableWithLineage) {
            final CommittableWithLineage<FileSinkCommittable> lineage =
                    (CommittableWithLineage<FileSinkCommittable>) message;
            committables.add(lineage);
            LOG.debug(
                    "Received CommittableWithLineage for checkpoint {}: subtaskId={}, "
                            + "committablesReceived={}",
                    lineage.getCheckpointId(),
                    lineage.getSubtaskId(),
                    committables.size());
        } else {
            throw new IllegalStateException(
                    "Unknown CommittableMessage subtype: " + message.getClass().getName());
        }
    }

    /**
     * Returns true once a summary has been received from every subtask and the total number of
     * committables matches the count declared by the summaries.
     *
     * <p>Returns false until the first summary arrives - committables could theoretically
     * interleave ahead of their summary, so {@code numberOfSubtasks} is unknown until then.
     */
    boolean isComplete() {
        final Integer numberOfSubtasks =
                summaries.stream()
                        .findFirst()
                        .map(CommittableSummary::getNumberOfSubtasks)
                        .orElse(null);

        final int expectedNumberOfCommittables =
                summaries.stream().mapToInt(CommittableSummary::getNumberOfCommittables).sum();

        return numberOfSubtasks != null
                && summaries.size() == numberOfSubtasks
                && committables.size() == expectedNumberOfCommittables;
    }

    /**
     * Returns the deduped pending files for this checkpoint.
     *
     * <p>Cleanup-only committables (in-progress or compacted) are skipped - the upstream {@link
     * org.apache.flink.connector.file.sink.committer.FileCommitter} has already deleted those files
     * synchronously during commit, so there is nothing left for the post-commit operator to do with
     * them. Pending files without a path are also skipped (nothing to load).
     */
    List<InProgressFileWriter.PendingFileRecoverable> collectFiles() {
        return committables.stream()
                .map(CommittableWithLineage::getCommittable)
                .filter(FileSinkCommittable::hasPendingFile)
                .map(FileSinkCommittable::getPendingFile)
                .filter(pendingFile -> pendingFile.getPath() != null)
                .collect(Collectors.toList());
    }
}
