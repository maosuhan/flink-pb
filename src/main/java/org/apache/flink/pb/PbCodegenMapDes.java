package org.apache.flink.pb;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

public class PbCodegenMapDes implements PbCodegenDes {
    private Descriptors.FieldDescriptor fd;
    private MapType mapType;
    private boolean ignoreDefaultValues;

    public PbCodegenMapDes(Descriptors.FieldDescriptor fd, MapType mapType, boolean ignoreDefaultValues) {
        this.fd = fd;
        this.mapType = mapType;
        this.ignoreDefaultValues = ignoreDefaultValues;
    }

    @Override
    public String codegen(String returnVarName, String messageGetStr) {
        CodegenVarUid varUid = CodegenVarUid.getInstance();
        int uid = varUid.getAndIncrement();

        LogicalType keyType = mapType.getKeyType();
        LogicalType valueType = mapType.getValueType();
        Descriptors.FieldDescriptor keyFd = fd.getMessageType().findFieldByName(PbConstant.PB_MAP_KEY_NAME);
        Descriptors.FieldDescriptor valueFd = fd.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME);

        StringBuilder sb = new StringBuilder();
        String keyJavaType = PbDesSerUtils.getTypeStrFromFD(keyFd);
        String valueJavaType = PbDesSerUtils.getTypeStrFromFD(valueFd);
        sb.append("Map<" + keyJavaType + "," + valueJavaType + "> map" + uid + " = " + messageGetStr + ";");
        sb.append("Map resultMap" + uid + " = new HashMap();");
        sb.append("for(Map.Entry<" + keyJavaType + "," + valueJavaType + "> entry" + uid + ": map" + uid + ".entrySet()){");
        String keyReturnVar = "keyReturnVar" + uid;
        String valueReturnVar = "valueReturnVar" + uid;
        sb.append("Object " + keyReturnVar + "= null;");
        sb.append("Object " + valueReturnVar + "= null;");
        PbCodegenDes keyDes = PbCodegenDesFactory.getPbCodegenDes(keyFd, keyType, ignoreDefaultValues);
        PbCodegenDes valueDes = PbCodegenDesFactory.getPbCodegenDes(valueFd, valueType, ignoreDefaultValues);
        sb.append(keyDes.codegen(keyReturnVar, "((" + keyJavaType + ")entry" + uid + ".getKey())"));
        sb.append(valueDes.codegen(valueReturnVar, "((" + valueJavaType + ")entry" + uid + ".getValue())"));
        sb.append("resultMap" + uid + ".put(" + keyReturnVar + ", " + valueReturnVar + ");");
        sb.append("}");
        sb.append(returnVarName + " = new GenericMapData(resultMap" + uid + ");");
        return sb.toString();
    }

}
