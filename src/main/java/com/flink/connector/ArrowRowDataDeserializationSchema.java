package com.flink.connector;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.readers.RowArrowReader;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import com.arrow.utils.ArrowSchemaConverter;
import com.arrow.utils.ArrowToRowDataConverter;


/** It takes {@link RowData} as the input type. */

public class ArrowRowDataDeserializationSchema implements DeserializationSchema<RowData>,Serializable {

	public static final long serialVersionUID = 1L;
	public TypeInformation<RowData> typeInfo;
	@SuppressWarnings("rawtypes")
	public DeserializationSchema<VectorSchemaRoot> nestedSchema;
	public ArrowToRowDataConverter runtimeConverter;
	List<GenericRowData> rowDataList;
	public RowArrowReader reader;
	public static Schema arrowSchema ;
	public ArrowRowDataDeserializationSchema(RowType rowType,
			TypeInformation<RowData> typeInfo) {
		this.typeInfo = typeInfo;
		this.arrowSchema = ArrowSchemaConverter.convertToSchema(rowType);
		this.nestedSchema=ArrowDeserializationSchema.forGeneric(arrowSchema,typeInfo);
		this.runtimeConverter = ArrowToRowDataConverter.createRowConverter(rowType);
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return typeInfo;
	}
    @Override
    public void open(InitializationContext context) throws Exception {
        this.nestedSchema.open(context);
    }
    
	@SuppressWarnings("unchecked")
	@Override
	public void deserialize(@Nullable byte[] message, Collector<RowData> out) throws IOException {
		if (message == null) {
			out = null;
		}
		try {
			VectorSchemaRoot root = nestedSchema.deserialize(message);
			List<GenericRowData> rowdatalist = (List<GenericRowData>)runtimeConverter.convert(root);
			for (int i=0;i<rowdatalist.size();i++) {
				out.collect(rowdatalist.get(i));
			}
			ArrowDeserializationSchema.close();
		} catch (Exception e) {
			throw new IOException("Failed to deserialize Arrow record.", e);
		}
	}

	public static Schema getArrowSchema() {
		return arrowSchema;
	}
	
	@Override
	 public RowData deserialize(@Nullable byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        RowData rowData;
		try {
        	VectorSchemaRoot root = nestedSchema.deserialize(message);
        	rowData= (RowData) runtimeConverter.convert(root);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize Arrow record.", e);
        }
		return rowData;
    }
	 @Override
	    public boolean equals(Object o) {
	        if (this == o) {
	            return true;
	        }
	        if (o == null || getClass() != o.getClass()) {
	            return false;
	        }
	        ArrowRowDataDeserializationSchema that = (ArrowRowDataDeserializationSchema) o;
	        return nestedSchema.equals(that.nestedSchema) && typeInfo.equals(that.typeInfo);
	    }

	    @Override
	    public int hashCode() {
	        return Objects.hash(nestedSchema, typeInfo);
	    }
	@Override
	public boolean isEndOfStream(RowData nextElement) {
		// TODO Auto-generated method stub
		return false;
	}

}