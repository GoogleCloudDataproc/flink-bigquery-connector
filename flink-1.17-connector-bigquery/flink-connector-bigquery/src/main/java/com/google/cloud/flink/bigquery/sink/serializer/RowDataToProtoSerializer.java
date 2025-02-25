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

package com.google.cloud.flink.bigquery.sink.serializer;

import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import com.google.api.client.util.Preconditions;
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Serializer for converting Flink's {@link RowData} to BigQuery proto. */
public class RowDataToProtoSerializer extends BigQueryProtoSerializer<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(RowDataToProtoSerializer.class);

    private final LogicalType type;
    private Descriptor descriptor;

    public RowDataToProtoSerializer(LogicalType type) {
        this.type = type;
    }

    /**
     * Prepares the serializer before its serialize method can be called. It allows contextual
     * preprocessing after constructor and before serialize. The Sink will internally call this
     * method when initializing itself.
     *
     * @param bigQuerySchemaProvider {@link BigQuerySchemaProvider} for the destination table.
     */
    @Override
    public void init(BigQuerySchemaProvider bigQuerySchemaProvider) {
        Preconditions.checkNotNull(
                bigQuerySchemaProvider,
                "BigQuerySchemaProvider not found while initializing AvroToProtoSerializer");
        Descriptor derivedDescriptor = bigQuerySchemaProvider.getDescriptor();
        Preconditions.checkNotNull(
                derivedDescriptor, "Destination BigQuery table's Proto Schema could not be found.");
        this.descriptor = derivedDescriptor;
    }

    @Override
    public Schema getAvroSchema(RowData record) {
        // Doesn't actually depend on record
        return AvroSchemaConverter.convertToSchema(this.type);
    }

    @Override
    public ByteString serialize(RowData record) throws BigQuerySerializationException {
        try {
            return getDynamicMessageFromRowData(record, this.descriptor, this.type).toByteString();
        } catch (Exception e) {
            throw new BigQuerySerializationException(
                    String.format(
                            "Error while serialising Row Data record: %s\nError: %s",
                            record, e.getMessage()),
                    e);
        }
    }

    public Object toProtoValue(
            LogicalType fieldType,
            int fieldNumber,
            RowData element,
            FieldDescriptor fieldDescriptor) {
        try {
            // Logical Type contains the supposed type (since it is formed from BQ Table)
            switch (fieldType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return element.getString(fieldNumber).toString();
                case BOOLEAN:
                    return element.getBoolean(fieldNumber);
                case BINARY:
                case VARBINARY:
                    return ByteString.copyFrom(element.getBinary(fieldNumber));
                case DECIMAL:
                    // Get the scale and precision.
                    DecimalType decimalType = (DecimalType) fieldType;
                    int precision = decimalType.getPrecision();
                    int scale = decimalType.getScale();
                    BigDecimal decimalValue =
                            element.getDecimal(fieldNumber, precision, scale).toBigDecimal();
                    return BigDecimalByteStringEncoder.encodeToNumericByteString(decimalValue);
                case TINYINT:
                case SMALLINT:
                    return Long.valueOf(String.valueOf((int) element.getShort(fieldNumber)));
                case INTEGER:
                case DATE:
                    // read in the form of - number of days since EPOCH (Integer)
                    return Long.valueOf(String.valueOf(element.getInt(fieldNumber)));
                case BIGINT:
                    return element.getLong(fieldNumber);
                case FLOAT:
                    return Double.valueOf(String.valueOf(element.getFloat(fieldNumber)));
                case DOUBLE:
                    return element.getDouble(fieldNumber);
                case ROW:
                    RowData row = element.getRow(fieldNumber, fieldType.getChildren().size());
                    return getDynamicMessageFromRowData(
                            row, fieldDescriptor.getMessageType(), fieldType);
                case TIME_WITHOUT_TIME_ZONE:
                    // case time
                    // Microseconds since MIDNIGHT
                    if (((TimeType) fieldType).getPrecision() == 3) {
                        return AvroToProtoSerializer.AvroSchemaHandler.convertTime(
                                element.getInt(fieldNumber), false);
                    } else {
                        return AvroToProtoSerializer.AvroSchemaHandler.convertTime(
                                element.getLong(fieldNumber), true);
                    }
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    // TIMESTAMP in BQ.
                    // microseconds since epoch
                    if (((TimestampType) fieldType).getPrecision() == 3) {
                        return AvroToProtoSerializer.AvroSchemaHandler.convertTimestamp(
                                element.getTimestamp(fieldNumber, 3).getMillisecond(),
                                false,
                                "Timestamp(millis)");
                    } else {
                        TimestampData timestampData = element.getTimestamp(fieldNumber, 6);
                        long micros = getMicrosFromTsData(timestampData);
                        return AvroToProtoSerializer.AvroSchemaHandler.convertTimestamp(
                                micros, true, "Timestamp(micros)");
                    }
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    // microseconds since epoch
                    if (((LocalZonedTimestampType) fieldType).getPrecision() == 3) {
                        return AvroToProtoSerializer.AvroSchemaHandler.convertDateTime(
                                element.getTimestamp(fieldNumber, 3).getMillisecond(), false);
                    } else {
                        TimestampData timestampData = element.getTimestamp(fieldNumber, 6);
                        long micros = getMicrosFromTsData(timestampData);
                        return AvroToProtoSerializer.AvroSchemaHandler.convertDateTime(
                                micros, true);
                    }
                case ARRAY:
                    LogicalType arrayElementType = getArrayElementType(fieldType);
                    ArrayData.ElementGetter elementGetter =
                            ArrayData.createElementGetter(arrayElementType);
                    return Stream.iterate(0, pos -> pos + 1)
                            .limit(element.getArray(fieldNumber).size())
                            .map(
                                    pos ->
                                            convertArrayElement(
                                                    elementGetter,
                                                    element,
                                                    fieldNumber,
                                                    pos,
                                                    arrayElementType,
                                                    fieldDescriptor))
                            .collect(Collectors.toList());
                    // all the below types are not supported yet.
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_DAY_TIME:
                case MAP:
                case MULTISET:
                case NULL:
                case SYMBOL:
                case RAW:
                case DISTINCT_TYPE:
                case STRUCTURED_TYPE:
                case TIMESTAMP_WITH_TIME_ZONE:
                    // Since it is only a logical type and not supported in flink SQL
                    // (https://issues.apache.org/jira/browse/FLINK-20869)
                case UNRESOLVED:
                default:
                    String notSupportedError =
                            String.format(
                                    "Serialization to ByteString for the passed RowData type: '%s' is not "
                                            + "supported yet!",
                                    fieldType.getTypeRoot().toString());
                    LOG.error(
                            String.format(
                                    "%s%nSupported types are: %s.",
                                    notSupportedError,
                                    "CHAR, VARCHAR, BOOLEAN, BINARY, VARBINARY,"
                                            + " DECIMAL, TINYINT, SMALLINT, INTEGER,"
                                            + " DATE, BIGINT, FLOAT, DOUBLE, ROW, TIME_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE,"
                                            + " TIMESTAMP_WITH_LOCAL_TIME_ZONE, and ARRAY"));
                    throw new UnsupportedOperationException(notSupportedError);
            }
        } catch (UnsupportedOperationException
                | ClassCastException
                | IllegalArgumentException
                | NullPointerException
                | IllegalStateException
                | IndexOutOfBoundsException
                | DateTimeException e) {
            String invalidError =
                    String.format(
                            "Error while converting RowData value '%s' to BigQuery Proto "
                                    + "Rows.%nError: %s",
                            element, e);
            LOG.error(
                    String.format(
                            "%s%nExpected Type: '%s' at Field Number '%d' for Logical Type: '%s'.%nError: %s",
                            invalidError,
                            fieldDescriptor.getType().name(),
                            fieldNumber,
                            fieldType.getTypeRoot().name(),
                            e));
            throw new IllegalArgumentException(invalidError);
        }
    }

    /**
     * Method to extract Array Element Type from ARRAY datatype.
     *
     * @param fieldType The ARRAY Datatype.
     * @return ARRAY element type.
     * @throws UnsupportedOperationException in case ARRAY has NULL datatype or multiple datatypes.
     */
    private static LogicalType getArrayElementType(LogicalType fieldType)
            throws UnsupportedOperationException {
        List<LogicalType> arrayElementTypes = fieldType.getChildren();
        if (fieldType.isNullable()) {
            throw new UnsupportedOperationException("NULLABLE ARRAY is not supported.");
        }
        // Will never be the case since LogicalType does not support ARRAY having multiple types.
        if (arrayElementTypes.size() > 1) {
            throw new UnsupportedOperationException(
                    "Multiple Datatypes not supported in ARRAY type");
        }
        LogicalType arrayElementType = arrayElementTypes.get(0);
        if (arrayElementType.getTypeRoot() == LogicalTypeRoot.NULL) {
            throw new UnsupportedOperationException("ARRAY of type NULL is not supported.");
        }
        return arrayElementType;
    }

    private long getMicrosFromTsData(TimestampData timestampData) {
        long millis = timestampData.getMillisecond();
        long nanos = timestampData.getNanoOfMillisecond();
        return TimeUnit.MILLISECONDS.toMicros(millis) + TimeUnit.NANOSECONDS.toMicros(nanos);
    }

    private Object convertArrayElement(
            ArrayData.ElementGetter elementGetter,
            RowData element,
            int fieldNumber,
            int pos,
            LogicalType arrayElementType,
            FieldDescriptor fieldDescriptor) {
        Object ele = elementGetter.getElementOrNull(element.getArray(fieldNumber), pos);
        return toProtoValue(arrayElementType, 0, GenericRowData.of(ele), fieldDescriptor);
    }

    /**
     * Function to convert a Generic Avro Record to Dynamic Message to write using the Storage Write
     * API.
     *
     * @param element {@link GenericRecord} Object to convert to {@link DynamicMessage}
     * @param descriptor {@link Descriptor} describing the schema of the sink table.
     * @return {@link DynamicMessage} Object converted from the Generic Avro Record.
     */
    public DynamicMessage getDynamicMessageFromRowData(
            RowData element, Descriptor descriptor, LogicalType type) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        // Get a record's field schema and find the field descriptor for each field one by
        // one.
        int fieldNumber = 0;
        for (LogicalType fieldType : type.getChildren()) {
            // In case no field descriptor exists for the field, throw an error as we have
            // incompatible schemas.
            Descriptors.FieldDescriptor fieldDescriptor =
                    Preconditions.checkNotNull(descriptor.findFieldByNumber(fieldNumber + 1));
            // Check if the value is null.
            if (element.isNullAt(fieldNumber)) {
                // Do nothing in case value == null and fieldDescriptor != "REQUIRED"
                if (fieldDescriptor.isRequired()) {
                    throw new IllegalArgumentException(
                            "Received null value for non-nullable field "
                                    + fieldDescriptor.getName());
                }
            } else {
                Object value = toProtoValue(fieldType, fieldNumber, element, fieldDescriptor);
                builder.setField(fieldDescriptor, value);
            }
            fieldNumber += 1;
        }
        return builder.build();
    }
}
