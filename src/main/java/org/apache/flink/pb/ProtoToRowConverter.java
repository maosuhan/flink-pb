package org.apache.flink.pb;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FileDescriptor.Syntax;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.codehaus.janino.ScriptEvaluator;

import java.lang.reflect.Method;

public class ProtoToRowConverter {
    private ScriptEvaluator se;
    private Method parseFromMethod;

    public ProtoToRowConverter(Class messageClass, RowType rowType, boolean ignoreDefaultValues) {
        try {
            Descriptors.Descriptor descriptor = PbDesSerUtils.getDescriptor(messageClass);
            if (descriptor.getFile().getSyntax() == Syntax.PROTO3) {
                ignoreDefaultValues = false;
            }
            se = new ScriptEvaluator();
            se.setParameters(new String[]{"message"}, new Class[]{messageClass});
            se.setReturnType(RowData.class);
            se.setDefaultImports(
                    "org.apache.flink.table.data.GenericRowData",
                    "org.apache.flink.table.data.GenericArrayData",
                    "org.apache.flink.table.data.GenericMapData",
                    "org.apache.flink.table.data.RowData",
                    "org.apache.flink.table.data.ArrayData",
                    "org.apache.flink.table.data.StringData",
                    "java.lang.Integer",
                    "java.lang.Long",
                    "java.lang.Float",
                    "java.lang.Double",
                    "java.lang.Boolean",
                    "java.util.ArrayList",
                    "java.sql.Timestamp",
                    "java.util.List",
                    "java.util.Map",
                    "java.util.HashMap");

            StringBuilder sb = new StringBuilder();
            sb.append("RowData rowData=null;");
            PbCodegenDes codegenDes = PbCodegenDesFactory.getPbCodegenTopRowDes(descriptor, rowType, ignoreDefaultValues);
            String genCode = codegenDes.codegen("rowData", "message");
            sb.append(genCode);
            sb.append("return rowData;");
            String code = sb.toString();

            System.out.println(code);

            se.cook(code);
            parseFromMethod = messageClass.getMethod(PbConstant.PB_METHOD_PARSE_FROM, byte[].class);
        } catch (Exception ex) {
            throw new PbDecodeCodegenException(ex);
        }
    }

    public RowData convertProtoBinaryToRow(byte[] data) throws Exception {
        Object messageObj = parseFromMethod.invoke(null, data);
        RowData ret = (RowData) se.evaluate(new Object[]{messageObj});
        return ret;
    }


}
