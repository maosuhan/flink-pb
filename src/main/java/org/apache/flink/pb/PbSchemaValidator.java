package org.apache.flink.pb;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PbSchemaValidator {
    private Descriptors.Descriptor descriptor;
    private RowType rowType;
    private Map<JavaType, List<LogicalTypeRoot>> typeMatchMap = new HashMap();

    public PbSchemaValidator(Descriptors.Descriptor descriptor, RowType rowType) {
        this.descriptor = descriptor;
        this.rowType = rowType;
        typeMatchMap.put(JavaType.BOOLEAN, Collections.singletonList(LogicalTypeRoot.BOOLEAN));
        typeMatchMap.put(JavaType.BYTE_STRING, Arrays.asList(LogicalTypeRoot.BINARY, LogicalTypeRoot.VARBINARY));
        typeMatchMap.put(JavaType.DOUBLE, Collections.singletonList(LogicalTypeRoot.DOUBLE));
        typeMatchMap.put(JavaType.FLOAT, Collections.singletonList(LogicalTypeRoot.FLOAT));
        typeMatchMap.put(JavaType.ENUM, Arrays.asList(LogicalTypeRoot.VARCHAR, LogicalTypeRoot.CHAR));
        typeMatchMap.put(JavaType.STRING, Arrays.asList(LogicalTypeRoot.VARCHAR, LogicalTypeRoot.CHAR));
        typeMatchMap.put(JavaType.INT, Collections.singletonList(LogicalTypeRoot.INTEGER));
        typeMatchMap.put(JavaType.LONG, Collections.singletonList(LogicalTypeRoot.BIGINT));
    }

    public Descriptors.Descriptor getDescriptor() {
        return descriptor;
    }

    public void setDescriptor(Descriptors.Descriptor descriptor) {
        this.descriptor = descriptor;
    }

    public RowType getRowType() {
        return rowType;
    }

    public void setRowType(RowType rowType) {
        this.rowType = rowType;
    }

    public void validate() {
        validateTypeMatch(descriptor, rowType);
    }


    public void validateTypeMatch(Descriptors.Descriptor descriptor, RowType rowType) {
        rowType.getFields().forEach(field -> {
            FieldDescriptor fieldDescriptor = descriptor.findFieldByName(field.getName());
            validateTypeMatch(fieldDescriptor, field.getType());
        });
    }

    public void validateTypeMatch(FieldDescriptor fd, LogicalType logicalType) {
        if (!fd.isRepeated()) {
            if (fd.getJavaType() != JavaType.MESSAGE) {
                //simple type
                validateSimpleType(fd, logicalType.getTypeRoot());
            } else {
                //message type
                validateTypeMatch(fd.getMessageType(), (RowType) logicalType);
            }
        } else {
            if (fd.isMapField()) {
                //map type
                MapType mapType = (MapType) logicalType;
                validateSimpleType(fd.getMessageType().findFieldByName(PbConstant.PB_MAP_KEY_NAME), mapType.getKeyType().getTypeRoot());
                validateTypeMatch(fd.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME), mapType.getValueType());
            } else {
                // array type
                ArrayType arrayType = (ArrayType) logicalType;
                if (fd.getJavaType() == JavaType.MESSAGE) {
                    //array message type
                    validateTypeMatch(fd.getMessageType(), (RowType) arrayType.getElementType());
                } else {
                    //array simple type
                    validateSimpleType(fd, arrayType.getElementType().getTypeRoot());
                }
            }

        }
    }

    private void validateSimpleType(FieldDescriptor fd, LogicalTypeRoot logicalTypeRoot) {
        if (!typeMatchMap.containsKey(fd.getJavaType())) {
            throw new ValidationException("unsupported protobuf java type: " + fd.getJavaType());
        }
        if (!typeMatchMap.get(fd.getJavaType()).stream().filter(x -> x == logicalTypeRoot).findAny().isPresent()) {
            throw new ValidationException("protobuf field type does not match column type, " + fd.getJavaType() + "(pb) is not compatible of " + logicalTypeRoot + "(table)");
        }
    }
}
