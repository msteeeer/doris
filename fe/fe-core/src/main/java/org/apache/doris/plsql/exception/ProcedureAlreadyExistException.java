package org.apache.doris.plsql.exception;

public class ProcedureAlreadyExistException extends ProcedureRuntimeException {
    public ProcedureAlreadyExistException(String procName) {
        super("Procedure " + procName + " already exist.");
    }
}
