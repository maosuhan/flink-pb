package org.apache.flink.pb;

public class PbDecodeCodegenException extends RuntimeException {
    public PbDecodeCodegenException() {
    }

    public PbDecodeCodegenException(String message) {
        super(message);
    }

    public PbDecodeCodegenException(String message, Throwable cause) {
        super(message, cause);
    }

    public PbDecodeCodegenException(Throwable cause) {
        super(cause);
    }

    public PbDecodeCodegenException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
