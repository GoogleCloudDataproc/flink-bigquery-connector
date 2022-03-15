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

import org.apache.arrow.vector.IntVector;
import org.apache.flink.table.data.vector.IntColumnVector;
import org.apache.flink.util.Preconditions;

/** Arrow column vector for Int. */

public final class ArrowIntColumnVector implements IntColumnVector {

	private final IntVector intVector;

	public ArrowIntColumnVector(IntVector intVector) {
		this.intVector = Preconditions.checkNotNull(intVector, "IntVector is null");
	}

	@Override
	public int getInt(int i) {
		return intVector.get(i);
	}

	@Override
	public boolean isNullAt(int i) {
		return intVector.isNull(i);
	}
}
