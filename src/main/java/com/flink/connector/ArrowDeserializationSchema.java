package com.flink.connector;


import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
/**
 * @param <T> type of the input elements.
 */
public class ArrowDeserializationSchema<T> implements DeserializationSchema<T>,Serializable {
	private static final long serialVersionUID = 1L;

	private BufferAllocator allocator;
	private final TypeInformation<RowData> typeInfo;
	static ArrowRecordBatch deserializedBatch;
	public static ArrowDeserializationSchema<VectorSchemaRoot> forGeneric(Schema schema,TypeInformation<RowData> typeInfo) {
		return new ArrowDeserializationSchema<>(VectorSchemaRoot.class,schema,typeInfo);
	}
	private static VectorSchemaRoot root;
	private VectorLoader loader;
	List<FieldVector> vectors = new ArrayList<>();
	private Schema schema;
	private final Class<T> recordClazz;

	ArrowDeserializationSchema(Class<T> recordClazz,Schema schema,TypeInformation<RowData> typeInfo) {
        Preconditions.checkNotNull(recordClazz, "Arrow record class must not be null.");
		this.typeInfo=typeInfo;
        this.recordClazz = recordClazz;
    }

    @SuppressWarnings("unchecked")
	@Override
    public T deserialize(byte[] message) throws IOException {
    	if (schema == null) {
			this.schema=ArrowRowDataDeserializationSchema.getArrowSchema();
		}
		checkArrowInitialized();
		deserializedBatch = MessageSerializer
				.deserializeRecordBatch(new ReadChannel(new ByteArrayReadableSeekableByteChannel(message)), allocator);
		loader.load(deserializedBatch);
		return (T) root;
	}
	void checkArrowInitialized() {
		Preconditions.checkNotNull(schema);
		if (root != null) {
			return;
		}
		if (allocator == null) {
			this.allocator = new RootAllocator(Long.MAX_VALUE);
		}
	      for (Field field : schema.getFields()) {
				vectors.add(field.createVector(allocator));
			}
			this.root = new VectorSchemaRoot(vectors);
			this.loader = new VectorLoader(root);
	}

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public TypeInformation<T> getProducedType() {
    	return (TypeInformation<T>) typeInfo;
        }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ArrowDeserializationSchema<?> that = (ArrowDeserializationSchema<?>) o;
        return recordClazz.equals(that.recordClazz) && Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordClazz, schema);
    }
    
    public static void close() {
    	deserializedBatch.close();
	    root.clear();
    }

}
