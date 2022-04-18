/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.flink.bigquery.arrow.util;

import org.apache.arrow.vector.TinyIntVector;
import org.apache.flink.table.data.vector.ByteColumnVector;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for TinyInt. */

public final class ArrowTinyIntColumnVector implements ByteColumnVector {

	private final TinyIntVector tinyIntVector;

	public ArrowTinyIntColumnVector(TinyIntVector tinyIntVector) {
		this.tinyIntVector = Preconditions.checkNotNull(tinyIntVector, "TinyIntVector is null");
	}

	@Override
	public byte getByte(int i) {
		return tinyIntVector.get(i);
	}

	@Override
	public boolean isNullAt(int i) {
		return tinyIntVector.isNull(i);
	}
}
