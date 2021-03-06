package org.apache.flink.pb;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.WireFormat;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class RowToProtoByteArray {
    private static Logger LOG = LoggerFactory.getLogger(RowToProtoByteArray.class);

    protected RowType rowType;
    protected List<ProtoSchemaMeta> protoSchemaMetaList = new ArrayList<>();
    protected Descriptors.Descriptor descriptor;
    protected Map<Integer, RowToProtoByteArray> protoIndexToConverterMap = new HashMap<>();

    public RowToProtoByteArray(RowType rowType, Descriptors.Descriptor descriptor) {
        this.rowType = rowType;
        this.descriptor = descriptor;

        for (int schemaFieldIndex = 0; schemaFieldIndex < rowType.getFields().size(); schemaFieldIndex++) {
            //defined in proto and also defined in row schema
            RowType.RowField rowField = rowType.getFields().get(schemaFieldIndex);
            Descriptors.FieldDescriptor field = descriptor.findFieldByName(rowField.getName());
            checkNotNull(field);
            LogicalType fieldType = rowField.getType();
            protoSchemaMetaList.add(new ProtoSchemaMeta(schemaFieldIndex, field, fieldType));
            if (field.getJavaType() == JavaType.MESSAGE) {
                if (field.isMapField()) {
                    //must be map type
                    MapType mapType = (MapType) fieldType;
                    Descriptors.FieldDescriptor valueFd = field.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME);
                    if (valueFd.getJavaType() == JavaType.MESSAGE) {
                        //only value of a map type can be message
                        RowToProtoByteArray sparkRowToProtoByteArray = new RowToProtoByteArray((RowType) mapType.getValueType(), valueFd.getMessageType());
                        protoIndexToConverterMap.put(field.getNumber(), sparkRowToProtoByteArray);
                    }
                } else if (field.isRepeated()) {
                    //must be list message type
                    ArrayType subArrayType = (ArrayType) fieldType;
                    RowToProtoByteArray sparkRowToProtoByteArray = new RowToProtoByteArray((RowType) subArrayType.getElementType(), field.getMessageType());
                    protoIndexToConverterMap.put(field.getNumber(), sparkRowToProtoByteArray);
                } else {
                    //message type
                    RowToProtoByteArray sparkRowToProtoByteArray = new RowToProtoByteArray((RowType) fieldType, field.getMessageType());
                    protoIndexToConverterMap.put(field.getNumber(), sparkRowToProtoByteArray);
                }
            }
        }
    }

    public byte[] convertToByteArray(RowData row) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream stream = CodedOutputStream.newInstance(baos);
        try {
            for (ProtoSchemaMeta protoSchemaMeta : protoSchemaMetaList) {
                Descriptors.FieldDescriptor fd = protoSchemaMeta.getFd();
                int schemaIndex = protoSchemaMeta.getSchemeIndex();
                LogicalType fieldLogicalType = protoSchemaMeta.getLogicalType();
                if (!row.isNullAt(schemaIndex)) {
                    //field value must not be null
                    if (!fd.isRepeated() && !fd.isMapField()) {
                        //row or simple type
                        stream.writeTag(fd.getNumber(), fd.getLiteType().getWireType());
                        if (fd.getJavaType() == JavaType.MESSAGE) {
                            //row type
                            RowData.FieldGetter fieldGetter = RowData.createFieldGetter(fieldLogicalType, schemaIndex);
                            RowData subRowData = (RowData) fieldGetter.getFieldOrNull(row);
                            RowToProtoByteArray subRowToProtoByteArray = protoIndexToConverterMap.get(fd.getNumber());
                            writeMessage(stream, subRowToProtoByteArray, subRowData);
                        } else {
                            //simple type
                            writeSimpleObj(stream, row, schemaIndex, fd);
                        }
                    } else if (fd.isMapField()) {
                        //map type
                        Descriptors.FieldDescriptor keyFd = fd.getMessageType().findFieldByName(PbConstant.PB_MAP_KEY_NAME);
                        Descriptors.FieldDescriptor valueFd = fd.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME);

                        MapType mapType = (MapType) protoSchemaMeta.getLogicalType();
                        MapData map = row.getMap(schemaIndex);
                        ArrayData keys = map.keyArray();
                        ArrayData values = map.valueArray();
                        for (int i = 0; i < keys.size(); i++) {
                            stream.writeTag(fd.getNumber(), WireFormat.WIRETYPE_LENGTH_DELIMITED);

                            ByteArrayOutputStream entryBaos = new ByteArrayOutputStream();
                            CodedOutputStream entryStream = CodedOutputStream.newInstance(entryBaos);

                            if (!keys.isNullAt(i)) {
                                //write key because it is not null
                                entryStream.writeTag(PbConstant.PB_MAP_KEY_TAG, keyFd.getLiteType().getWireType());
                                writeSimpleObj(entryStream, keys, i, keyFd);
                            }
                            if (!values.isNullAt(i)) {
                                //write value because it is not null
                                entryStream.writeTag(PbConstant.PB_MAP_VALUE_TAG, valueFd.getLiteType().getWireType());
                                if (valueFd.getJavaType() == JavaType.MESSAGE) {
                                    ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(mapType.getValueType());
                                    writeMessage(entryStream, protoIndexToConverterMap.get(fd.getNumber()), elementGetter.getElementOrNull(values, i));
                                } else {
                                    writeSimpleObj(entryStream, values, i, valueFd);
                                }
                            }
                            entryStream.flush();
                            byte[] entryData = entryBaos.toByteArray();
                            stream.writeUInt32NoTag(entryData.length);
                            stream.writeRawBytes(entryData);
                        }
                    } else if (fd.isRepeated()) {
                        //repeated row or repeated simple type
                        ArrayData objs = row.getArray(schemaIndex);
                        for (int j = 0; j < objs.size(); j++) {
                            stream.writeTag(fd.getNumber(), fd.getLiteType().getWireType());
                            if (fd.getJavaType() == JavaType.MESSAGE) {
                                //repeated row
                                ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(((ArrayType) fieldLogicalType).getElementType());
                                Object messageElement = elementGetter.getElementOrNull(objs, j);
                                if(null == messageElement){
                                    writeNullMessage(stream, fd.getMessageType());
                                }else{
                                    writeMessage(stream, protoIndexToConverterMap.get(fd.getNumber()), messageElement);
                                }
                            } else {
                                //repeated simple type
                                writeSimpleObj(stream, objs, j, fd);
                            }
                        }
                    }
                }
            }
            stream.flush();
            byte[] bytes = baos.toByteArray();
            return bytes;
        } catch (IOException ex) {
            throw new ProtobufDirectOutputStreamException(ex);
        }
    }


    //the field must not be null
    private void writeSimpleObj(CodedOutputStream stream, RowData row, int pos, Descriptors.FieldDescriptor fd) throws IOException {
        switch (fd.getJavaType()) {
            case STRING:
                stream.writeStringNoTag(row.getString(pos).toString());
                break;
            case INT:
                stream.writeInt32NoTag(row.getInt(pos));
                break;
            case LONG:
                stream.writeInt64NoTag(row.getLong(pos));
                break;
            case FLOAT:
                stream.writeFloatNoTag(row.getFloat(pos));
                break;
            case DOUBLE:
                stream.writeDoubleNoTag(row.getDouble(pos));
                break;
            case BOOLEAN:
                stream.writeBoolNoTag(row.getBoolean(pos));
                break;
            case BYTE_STRING:
                stream.writeByteArrayNoTag(row.getBinary(pos));
                break;
            case ENUM:
                stream.writeEnumNoTag(fd.getEnumType().findValueByName(row.getString(pos).toString()).getNumber());
                break;
        }
    }

    private void writeSimpleObj(CodedOutputStream stream, ArrayData array, int pos, Descriptors.FieldDescriptor fd) throws IOException {
        switch (fd.getJavaType()) {
            case STRING:
                stream.writeStringNoTag(array.getString(pos).toString());
                break;
            case INT:
                stream.writeInt32NoTag(array.getInt(pos));
                break;
            case LONG:
                stream.writeInt64NoTag(array.getLong(pos));
                break;
            case FLOAT:
                stream.writeFloatNoTag(array.getFloat(pos));
                break;
            case DOUBLE:
                stream.writeDoubleNoTag(array.getDouble(pos));
                break;
            case BOOLEAN:
                stream.writeBoolNoTag(array.getBoolean(pos));
                break;
            case BYTE_STRING:
                stream.writeByteArrayNoTag(array.getBinary(pos));
                break;
            case ENUM:
                Descriptors.EnumValueDescriptor enumValueDescriptor = fd.getEnumType().findValueByName(array.getString(pos).toString());
                if (null == enumValueDescriptor) {
                    //cannot find correspond enum value
                    int firstEnumTagNum = fd.getEnumType().getValues().get(0).getNumber();
                    stream.writeEnumNoTag(firstEnumTagNum);
                } else {
                    //can find correspond enum value
                    stream.writeEnumNoTag(enumValueDescriptor.getNumber());
                }
                break;
        }
    }

    private void writeMessage(CodedOutputStream stream, RowToProtoByteArray rowToProtoByteArray, Object obj) throws IOException {
        byte[] subBytes = rowToProtoByteArray.convertToByteArray((RowData) obj);
        stream.writeUInt32NoTag(subBytes.length);
        stream.writeRawBytes(subBytes);
    }

    private void writeNullMessage(CodedOutputStream stream, Descriptors.Descriptor messageType) throws IOException {
        byte[] subBytes = messageType.toProto().getDefaultInstanceForType().toByteArray();
        stream.writeUInt32NoTag(subBytes.length);
        stream.writeRawBytes(subBytes);
    }
}


class ProtoSchemaMeta implements Serializable {
    private int schemeIndex;
    private Descriptors.FieldDescriptor fd;
    private LogicalType logicalType;

    public ProtoSchemaMeta(int schemeIndex, Descriptors.FieldDescriptor fd, LogicalType logicalType) {
        this.schemeIndex = schemeIndex;
        this.fd = fd;
        this.logicalType = logicalType;
    }

    public int getSchemeIndex() {
        return schemeIndex;
    }

    public void setSchemeIndex(int schemeIndex) {
        this.schemeIndex = schemeIndex;
    }

    public Descriptors.FieldDescriptor getFd() {
        return fd;
    }

    public void setFd(Descriptors.FieldDescriptor fd) {
        this.fd = fd;
    }

    public LogicalType getLogicalType() {
        return logicalType;
    }

    public void setLogicalType(LogicalType logicalType) {
        this.logicalType = logicalType;
    }
}
