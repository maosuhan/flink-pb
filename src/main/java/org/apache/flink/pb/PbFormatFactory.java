package org.apache.flink.pb;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.util.HashSet;
import java.util.Set;

public class PbFormatFactory implements DeserializationFormatFactory, SerializationFormatFactory {
    public static final ConfigOption<String> MESSAGE_CLASS_NAME = ConfigOptions.key("message-class-name")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = ConfigOptions.key("ignore-parse-errors")
            .booleanType()
            .defaultValue(false);

    public static final ConfigOption<Boolean> IGNORE_DEFAULT_VALUES = ConfigOptions.key("ignore-default-values")
            .booleanType()
            .defaultValue(false);

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        final String messageClassName = formatOptions.get(MESSAGE_CLASS_NAME);
        boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        boolean ignoreDefaultValues = formatOptions.get(IGNORE_DEFAULT_VALUES);
        return new PbDecodingFormat(messageClassName, ignoreParseErrors, ignoreDefaultValues);
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        final String messageClassName = formatOptions.get(MESSAGE_CLASS_NAME);
        return new PbEncodingFormat(messageClassName);
    }

    @Override
    public String factoryIdentifier() {
        return "pb";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> result = new HashSet<>();
        result.add(MESSAGE_CLASS_NAME);
        return result;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> result = new HashSet<>();
        result.add(IGNORE_PARSE_ERRORS);
        result.add(IGNORE_DEFAULT_VALUES);
        return result;
    }

}
