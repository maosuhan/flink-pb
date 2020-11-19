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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;


@PublicEvolving
public class PbRowDeserializationSchema implements DeserializationSchema<RowData> {

    private static Logger LOG = LoggerFactory.getLogger(PbRowDeserializationSchema.class);
    private static final long serialVersionUID = -4040917522067315718L;


    private final RowType rowType;

    private final String messageClassName;

    /**
     * Flag indicating whether to ignore invalid fields/rows (default: throw an exception).
     */
    private final boolean ignoreParseErrors;
    private final boolean ignoreDefaultValues;

    private transient ProtoToRowConverter protoToRowConverter;

    public PbRowDeserializationSchema(RowType rowType, String messageClassName, boolean ignoreParseErrors, boolean ignoreDefaultValues) {
        checkNotNull(rowType, "Type information");
        this.rowType = rowType;
        this.messageClassName = messageClassName;
        this.ignoreParseErrors = ignoreParseErrors;
        this.ignoreDefaultValues = ignoreDefaultValues;
        Class messageClass;
        try {
            //do it in client side to report error in the first place
            messageClass = Class.forName(messageClassName);
            PbSchemaValidator pbSchemaValidator = new PbSchemaValidator(PbDesSerUtils.getDescriptor(messageClass), rowType);
            pbSchemaValidator.validate();
            protoToRowConverter = new ProtoToRowConverter(messageClass, rowType, ignoreDefaultValues);
        } catch (ClassNotFoundException e) {
            throw new ValidationException("message class not found", e);
        } catch (ReflectiveOperationException e) {
            throw new ValidationException("message class is not a standard protobuf class", e);
        }

    }

    public PbRowDeserializationSchema(RowType rowType, String messageClassName) {
        this(rowType, messageClassName, false, true);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        try {
            Class messageClass = Class.forName(messageClassName);
            protoToRowConverter = new ProtoToRowConverter(messageClass, rowType, ignoreDefaultValues);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("message class not found", e);
        }
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        try {
            return protoToRowConverter.convertProtoBinaryToRow(message);
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                return null;
            }
            LOG.error("Failed to deserialize PB object.", t);
            throw new IOException("Failed to deserialize PB object.", t);
        }
    }


    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return new RowDataTypeInfo(this.rowType);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PbRowDeserializationSchema that = (PbRowDeserializationSchema) o;
        return ignoreParseErrors == that.ignoreParseErrors &&
                Objects.equals(rowType, that.rowType) &&
                Objects.equals(messageClassName, that.messageClassName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowType, messageClassName, ignoreParseErrors);
    }
}
