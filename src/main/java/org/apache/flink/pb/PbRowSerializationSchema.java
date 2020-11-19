/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.pb;

import com.google.protobuf.Descriptors;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PbRowSerializationSchema implements SerializationSchema<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(PbRowSerializationSchema.class);

    private final RowType rowType;

    private final String messageClassName;

    private transient RowToProtoByteArray rowToProtoByteArray;


    public PbRowSerializationSchema(RowType rowType, String messageClassName) {
        this.rowType = rowType;
        this.messageClassName = messageClassName;
        Descriptors.Descriptor descriptor = PbDesSerUtils.getDescriptor(messageClassName);
        PbSchemaValidator pbSchemaValidator = new PbSchemaValidator(descriptor, rowType);
        pbSchemaValidator.validate();
        this.rowToProtoByteArray = new RowToProtoByteArray(rowType, PbDesSerUtils.getDescriptor(messageClassName));
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        this.rowToProtoByteArray = new RowToProtoByteArray(rowType, PbDesSerUtils.getDescriptor(messageClassName));
    }

    @Override
    public byte[] serialize(RowData element) {
        try {
            return rowToProtoByteArray.convertToByteArray(element);
        } catch (Exception e) {
            throw new ProtobufDirectOutputStreamException(e);
        }
    }
}
