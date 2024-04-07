package org.apache.doris.plsql.exception;

public class ProcedureNotExistException extends ProcedureRuntimeException {
    public ProcedureNotExistException(String msg) {
        super(msg);
    }
}
