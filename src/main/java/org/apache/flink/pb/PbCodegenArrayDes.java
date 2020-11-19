package org.apache.flink.pb;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.LogicalType;

public class PbCodegenArrayDes implements PbCodegenDes {
    private Descriptors.FieldDescriptor fd;
    private LogicalType elementType;
    private boolean ignoreDefaultValues;

    public PbCodegenArrayDes(Descriptors.FieldDescriptor fd, LogicalType elementType, boolean ignoreDefaultValues) {
        this.fd = fd;
        this.elementType = elementType;
        this.ignoreDefaultValues = ignoreDefaultValues;
    }

    @Override
    public String codegen(String returnVarName, String messageGetStr) {
        CodegenVarUid varUid = CodegenVarUid.getInstance();
        int uid = varUid.getAndIncrement();
        StringBuilder sb = new StringBuilder();
        String javaTypeStr = PbDesSerUtils.getTypeStrFromFD(fd);
        sb.append("List<" + javaTypeStr + "> list" + uid + "=" + messageGetStr + ";");
        sb.append("Object[] newObjs" + uid + "= new Object[list" + uid + ".size()];");
        sb.append("for(int i" + uid + "=0;i" + uid + " < list" + uid + ".size(); i" + uid + "++){");
        String subReturnVar = "returnVar" + uid;
        sb.append("Object " + subReturnVar + "=null;");
        PbCodegenDes codegenDes = PbCodegenDesFactory.getPbCodegenDes(fd, elementType, ignoreDefaultValues);
        sb.append(javaTypeStr + " subObj" + uid + " = (" + javaTypeStr + ")list" + uid + ".get(i" + uid + ");");
        String code = codegenDes.codegen(subReturnVar, "subObj" + uid);
        sb.append(code);
        sb.append("newObjs" + uid + "[i" + uid + "]=" + subReturnVar + ";");
        sb.append("}");
        sb.append(returnVarName + " = new GenericArrayData(newObjs" + uid + ");");
        return sb.toString();
    }

}
