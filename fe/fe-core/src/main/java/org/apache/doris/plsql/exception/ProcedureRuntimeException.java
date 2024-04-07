package org.apache.doris.plsql.exception;

public class ProcedureRuntimeException extends RuntimeException {
    public ProcedureRuntimeException(String msg) {
        super(msg);
    }
}
