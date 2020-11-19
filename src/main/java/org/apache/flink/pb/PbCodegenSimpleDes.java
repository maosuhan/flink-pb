package org.apache.flink.pb;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.LogicalType;

public class PbCodegenSimpleDes implements PbCodegenDes {
    private Descriptors.FieldDescriptor fd;
    private LogicalType type;
    private boolean ignoreDefaultValues;

    public PbCodegenSimpleDes(Descriptors.FieldDescriptor fd, LogicalType type, boolean ignoreDefaultValues) {
        this.fd = fd;
        this.type = type;
        this.ignoreDefaultValues = ignoreDefaultValues;
    }

    @Override
    public String codegen(String returnVarName, String messageGetStr) {
        StringBuilder sb = new StringBuilder();
        switch (fd.getJavaType()) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                sb.append(returnVarName + " = " + messageGetStr + ";");
                break;
            case BYTE_STRING:
                sb.append(returnVarName + " = " + messageGetStr + ".toByteArray();");
                break;
            case STRING:
            case ENUM:
                sb.append(returnVarName + " = StringData.fromString(" + messageGetStr + ".toString());");
                break;
        }
        return sb.toString();
    }
}
