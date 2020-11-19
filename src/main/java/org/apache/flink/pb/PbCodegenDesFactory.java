package org.apache.flink.pb;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

public class PbCodegenDesFactory {
    public static PbCodegenDes getPbCodegenDes(Descriptors.FieldDescriptor fd, LogicalType type, boolean ignoreDefaultValues) {
        if (type instanceof RowType) {
            return new PbCodegenRowDes(fd.getMessageType(), (RowType) type, ignoreDefaultValues);
        } else if (PbDesSerUtils.isSimpleType(type)) {
            return new PbCodegenSimpleDes(fd, type, ignoreDefaultValues);
        } else if (type instanceof ArrayType) {
            return new PbCodegenArrayDes(fd, ((ArrayType) type).getElementType(), ignoreDefaultValues);
        } else if (type instanceof MapType) {
            return new PbCodegenMapDes(fd, (MapType) type, ignoreDefaultValues);
        } else {
            throw new PbDecodeCodegenException("cannot support flink type: " + type);
        }
    }

    public static PbCodegenDes getPbCodegenTopRowDes(Descriptors.Descriptor descriptor, RowType rowType, boolean ignoreDefaultValues) {
        return new PbCodegenRowDes(descriptor, rowType, ignoreDefaultValues);
    }
}
