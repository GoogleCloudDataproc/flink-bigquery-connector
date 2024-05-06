package com.google.cloud.flink.bigquery.sink.serializer;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.api.client.util.Preconditions;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Javadoc. */
public class RowDataToProtoSerializer extends BigQueryProtoSerializer<RowData> {

    private Descriptor descriptor;

    private static final Logger LOG = LoggerFactory.getLogger(RowDataToProtoSerializer.class);

    private LogicalType type;

    public RowDataToProtoSerializer() {}

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

    public void setLogicalType(LogicalType type) {
        this.type = type;
    }

    @Override
    public ByteString serialize(RowData record) throws BigQuerySerializationException {
        try {
            return getDynamicMessageFromRowData(record, this.descriptor, this.type).toByteString();
        } catch (Exception e) {
            throw new BigQuerySerializationException(e.getMessage());
        }
    }

    public Object toProtoValue(
            LogicalType fieldType,
            int fieldNumber,
            RowData element,
            FieldDescriptor fieldDescriptor) {
        LOG.info(
                "In toProtoValue(), element: "
                        + element
                        + " ,fieldNumber: "
                        + fieldNumber
                        + ", fieldType: "
                        + fieldType);
        try {
            // Logical Type contains the supposed type (since it is formed from BQ Table)
            switch (fieldType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    LOG.info("In CHAR/VARCHAR: ");
                    return element.getString(fieldNumber).toString();
                case BOOLEAN:
                    return element.getBoolean(fieldNumber);
                case BINARY:
                case VARBINARY:
                    LOG.info("In VARBINARY/BINARY: ");
                    return ByteString.copyFrom(element.getBinary(fieldNumber));
                case DECIMAL:
                    LOG.info("In DECIMAL:");
                    LOG.info("element: " + element);
                    LOG.info("fieldType: " + fieldType);
                    LOG.info("fieldType.getTypeRoot(): " + fieldType.getTypeRoot());
                    LOG.info("fieldType.getChildren(): " + fieldType.getChildren());
                    LOG.info("fieldType.getChildren(): " + " ");
                    // TODO
                    return element.getDecimal(fieldNumber, 38, 9);
                case TINYINT:
                case SMALLINT:
                    return Short.toUnsignedInt(element.getShort(fieldNumber));
                case INTEGER:
                    return element.getInt(fieldNumber);
                case BIGINT:
                    LOG.info("In BIGINT : " + element.getLong(fieldNumber));
                    return element.getLong(fieldNumber);
                case FLOAT:
                    return element.getFloat(fieldNumber);
                case DOUBLE:
                    return element.getDouble(fieldNumber);
                case DATE:
                    LOG.info("In DATE: ");
                    LOG.info("element: " + element);
                    // read in the form of - number of days since EPOCH (Integer)
                    return element.getInt(fieldNumber);
                case ROW:
                    LOG.info("In ROW :");
                    RowData row = element.getRow(fieldNumber, fieldType.getChildren().size());
                    LOG.info("row: " + row);
                    LOG.info(
                            "fieldDescriptor.getMessageType(): "
                                    + fieldDescriptor.getMessageType());
                    return getDynamicMessageFromRowData(
                            row, fieldDescriptor.getMessageType(), fieldType);
                case TIME_WITHOUT_TIME_ZONE:
                    // case time
                    // TODO: TIME - read does not happen as
                    //  Storage Read API reads in microseconds since midnight (long value).
                    LOG.info("In TIME_WITHOUT_TIME_ZONE:");
                    LOG.info("element: " + element);
                    // TODO: Complete
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                    // case Timestamp
                    LOG.info("In TIMESTAMP_WITH_TIME_ZONE:");
                    LOG.info("element: " + element);
                    // TODO
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    // TIMESTAMP in BQ.
                    // microseconds since epoch
                    // Since the precision is set to 6,
                    // we get microsecond precision.
                    Long micros = element.getTimestamp(fieldNumber, 6).getMillisecond();
                    Long convertedTime =
                            AvroToProtoSerializer.AvroSchemaHandler.convertTimestamp(
                                    micros, true, "");
                    return convertedTime;
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    // TODO
                case ARRAY:
                    // TODO: Error for multiple children type and NULLABLE children type.
                    LOG.info("In ARRAY:");
                    LogicalType arrayElementType = fieldType.getChildren().get(0);
                    LOG.info("arrayElementType: " + arrayElementType);
                    ArrayData.ElementGetter elementGetter =
                            ArrayData.createElementGetter(arrayElementType);
                    List<Object> ans =
                            Stream.iterate(0, pos -> pos + 1)
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
                    LOG.info("ans: " + ans);
                    return ans;
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_DAY_TIME:
                case MAP:
                case MULTISET:
                case DISTINCT_TYPE:
                case STRUCTURED_TYPE:
                case NULL:
                case RAW:
                case SYMBOL:
                case UNRESOLVED:
                    throw new UnsupportedOperationException("Not supported yet!");
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Expected Type: %s at Field Number %d for Logical Type: %s.\nError: %s",
                            fieldDescriptor.getType().name(),
                            fieldNumber,
                            fieldType.getTypeRoot().name(),
                            e.getMessage()));
        }
        return null;
    }

    private Object convertArrayElement(
            ArrayData.ElementGetter elementGetter,
            RowData element,
            int fieldNumber,
            int pos,
            LogicalType arrayElementType,
            FieldDescriptor fieldDescriptor) {
        Object ele = elementGetter.getElementOrNull(element.getArray(fieldNumber), pos);
        LOG.info("ele: " + ele);
        Object converted =
                toProtoValue(arrayElementType, 0, GenericRowData.of(ele), fieldDescriptor);
        LOG.info("Converted: " + converted);
        return converted;
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
        LOG.info("In getDynamicMessageFromRowData()");
        for (LogicalType fieldType : type.getChildren()) {
            LOG.info("LogicalType fieldType: " + fieldType + " fieldNumber " + fieldNumber);
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
                LOG.info("Value: " + value);
                builder.setField(fieldDescriptor, value);
            }
            fieldNumber += 1;
        }
        return builder.build();
    }
}
