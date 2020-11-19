package org.apache.flink.pb;

import com.google.protobuf.Descriptors;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.LogicalType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class PbDesSerUtils {
    public static final String OUTER_CLASS = "OuterClass";


    //protobuf code has a bug that, f_abc_7d will be convert to fAbc7d, but actually we need fAbc7D
    public static String fieldNameToJsonName(String name) {
        final int length = name.length();
        StringBuilder result = new StringBuilder(length);
        boolean isNextUpperCase = false;
        for (int i = 0; i < length; i++) {
            char ch = name.charAt(i);
            if (ch == '_') {
                isNextUpperCase = true;
            } else if (isNextUpperCase) {
                // This closely matches the logic for ASCII characters in:
                // http://google3/google/protobuf/descriptor.cc?l=249-251&rcl=228891689
                if ('a' <= ch && ch <= 'z') {
                    ch = (char) (ch - 'a' + 'A');
                    isNextUpperCase = false;
                }
                result.append(ch);
            } else {
                result.append(ch);
            }
        }
        return result.toString();
    }

    public static boolean isSimpleType(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return true;
            default:
                return false;
        }
    }

    public static String getStrongCamelCaseJsonName(String name) {
        String jsonName = fieldNameToJsonName(name);
        if (jsonName.length() == 1) {
            return jsonName.toUpperCase();
        } else {
            return jsonName.substring(0, 1).toUpperCase() + jsonName.substring(1);
        }
    }

    public static String getTypeStrFromFD(Descriptors.FieldDescriptor fd) {
        switch (fd.getJavaType()) {
            case MESSAGE:
                return getJavaFullName(fd.getMessageType());
            case INT:
                return "Integer";
            case LONG:
                return "Long";
            case STRING:
            case ENUM:
                return "Object";
            case FLOAT:
                return "Float";
            case DOUBLE:
                return "Double";
            case BYTE_STRING:
                return "byte[]";
            case BOOLEAN:
                return "Boolean";
            default:
                throw new PbDecodeCodegenException("do not support field type: " + fd.getJavaType());
        }
    }

    public static String getJavaFullName(Descriptors.Descriptor descriptor) {
        String javaPackageName = descriptor.getFile().getOptions().getJavaPackage();
        if (descriptor.getFile().getOptions().getJavaMultipleFiles()) {
            //multiple_files=true
            if (null != descriptor.getContainingType()) {
                //nested type
                String parentJavaFullName = getJavaFullName(descriptor.getContainingType());
                return parentJavaFullName + "." + descriptor.getName();
            } else {
                //top level message
                return javaPackageName + "." + descriptor.getName();
            }
        } else {
            //multiple_files=false
            if (null != descriptor.getContainingType()) {
                //nested type
                String parentJavaFullName = getJavaFullName(descriptor.getContainingType());
                return parentJavaFullName + "." + descriptor.getName();
            } else {
                //top level message
                if (!descriptor.getFile().getOptions().hasJavaOuterClassname()) {
                    //user do not define outer class name in proto file
                    return javaPackageName + "." + descriptor.getName() + OUTER_CLASS + "." + descriptor.getName();
                } else {
                    String outerName = descriptor.getFile().getOptions().getJavaOuterClassname();
                    //user define outer class name in proto file
                    return javaPackageName + "." + outerName + "." + descriptor.getName();
                }
            }
        }
    }

    public static Descriptors.Descriptor getDescriptor(String className) {
        try {
            Class<?> pbClass = Class.forName(className);
            return (Descriptors.Descriptor) pbClass.getMethod(PbConstant.PB_METHOD_GET_DESCRIPTOR).invoke(null);
        } catch (Exception y) {
            throw new IllegalArgumentException(String.format("get %s descriptors error!", className), y);
        }
    }


}
