package org.apache.flink.pb;

public class ProtobufDirectOutputStreamException extends RuntimeException {
    public ProtobufDirectOutputStreamException() {
    }

    public ProtobufDirectOutputStreamException(String message) {
        super(message);
    }

    public ProtobufDirectOutputStreamException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProtobufDirectOutputStreamException(Throwable cause) {
        super(cause);
    }

    public ProtobufDirectOutputStreamException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
