package org.apache.flink.pb;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

public class PbCodegenRowDes implements PbCodegenDes {
    private List<Descriptors.FieldDescriptor> fds;
    private Descriptors.Descriptor descriptor;
    private RowType rowType;
    private boolean ignoreDefaultValues;

    public PbCodegenRowDes(Descriptors.Descriptor descriptor, RowType rowType, boolean ignoreDefaultValues) {
        this.fds = descriptor.getFields();
        this.rowType = rowType;
        this.descriptor = descriptor;
        this.ignoreDefaultValues = ignoreDefaultValues;
    }

    @Override
    public String codegen(String returnVarName, String messageGetStr) {
        VarUid varUid = VarUid.getInstance();
        int uid = varUid.getAndIncrement();
        StringBuilder sb = new StringBuilder();
        sb.append(PbDesSerUtils.getJavaFullName(descriptor) + " message" + uid + " = " + messageGetStr + ";");
        sb.append("GenericRowData rowData" + uid + " = new GenericRowData(" + rowType.getFieldNames().size() + ");");
        int index = 0;
        for (String fieldName : rowType.getFieldNames()) {
            LogicalType subType = rowType.getTypeAt(rowType.getFieldIndex(fieldName));
            Descriptors.FieldDescriptor subFd = fds.stream().filter(x -> x.getName().equals(fieldName)).findFirst().get();
            String strongCamelFieldName = PbDesSerUtils.getStrongCamelCaseJsonName(fieldName);
            PbCodegenDes codegen = PbCodegenDesFactory.getPbCodegenDes(subFd, subType, ignoreDefaultValues);
            int subUid = varUid.getAndIncrement();
            String returnVar = "returnVar" + subUid;
            sb.append("Object " + returnVar + " = null;");
            if (ignoreDefaultValues) {
                sb.append("if(" + isMessageNonEmptyStr("message" + uid, strongCamelFieldName, subFd) + "){");
            }
            String subMessageGetStr = getMessageGetStr("message" + uid, strongCamelFieldName, subFd);
            if (!subFd.isRepeated()) {
                //field is not map or array
                //this step is needed to convert primitive type to boxed type to unify the object interface
                switch (subFd.getJavaType()) {
                    case INT:
                        subMessageGetStr = "Integer.valueOf(" + subMessageGetStr + ")";
                        break;
                    case LONG:
                        subMessageGetStr = "Long.valueOf(" + subMessageGetStr + ")";
                        break;
                    case FLOAT:
                        subMessageGetStr = "Float.valueOf(" + subMessageGetStr + ")";
                        break;
                    case DOUBLE:
                        subMessageGetStr = "Double.valueOf(" + subMessageGetStr + ")";
                        break;
                    case BOOLEAN:
                        subMessageGetStr = "Boolean.valueOf(" + subMessageGetStr + ")";
                        break;
                }
            }

            String code = codegen.codegen(returnVar, subMessageGetStr);
            sb.append(code);
            if (ignoreDefaultValues) {
                sb.append("}");
            }
            sb.append("rowData" + uid + ".setField(" + index + ", " + returnVar + ");");
            index += 1;
        }
        sb.append(returnVarName + " = rowData" + uid + ";");
        return sb.toString();
    }

    private String getMessageGetStr(String message, String fieldName, Descriptors.FieldDescriptor fd) {
        if (fd.isMapField()) {
            return message + ".get" + fieldName + "Map()";
        } else if (fd.isRepeated()) {
            return message + ".get" + fieldName + "List()";
        } else {
            return message + ".get" + fieldName + "()";
        }
    }

    private String isMessageNonEmptyStr(String message, String fieldName, Descriptors.FieldDescriptor fd) {
        if (fd.isRepeated()) {
            return message + ".get" + fieldName + "Count() > 0";
        } else {
            return message + ".has" + fieldName + "()";
        }
    }
}
