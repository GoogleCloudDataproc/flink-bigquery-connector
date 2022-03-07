package com.flink.connector;

import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableModifiers;
import com.google.protobuf.Timestamp;

public class BiqQueryReadSession {

	public String createReadSession(BigQueryReadClient client,String project_id, String dataset, String table,Integer snapshotMillis) {	
		Integer v_snapshotMillis;
		String parent = String.format("projects/%s", project_id);
		String srcTable = String.format("projects/%s/datasets/%s/tables/%s", project_id, dataset, table);
 		if (snapshotMillis != 0) {
			v_snapshotMillis = snapshotMillis;
		} else {
			v_snapshotMillis = null;
		}
		ReadSession.Builder sessionBuilder = ReadSession.newBuilder().setTable(srcTable)
				.setDataFormat(DataFormat.ARROW);
		if (v_snapshotMillis != null) {
			Timestamp t = Timestamp.newBuilder().setSeconds(snapshotMillis / 1000)
					.setNanos((int) ((snapshotMillis % 1000) * 1000000)).build();
			TableModifiers modifiers = TableModifiers.newBuilder().setSnapshotTime(t).build();
			sessionBuilder.setTableModifiers(modifiers);
		}
		
		CreateReadSessionRequest.Builder builder = CreateReadSessionRequest.newBuilder().setParent(parent)
				.setReadSession(sessionBuilder).setMaxStreamCount(1);
		ReadSession session = client.createReadSession(builder.build());
	    Preconditions.checkState(session.getStreamsCount() > 0);
		String streamName = session.getStreams(0).getName();
		System.out.println("Stream Name  : "+streamName);
		return streamName;
	}
}