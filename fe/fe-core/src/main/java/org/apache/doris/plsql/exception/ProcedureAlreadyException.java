package org.apache.doris.plsql.exception;

public class ProcedureAlreadyException extends RuntimeException {
    public ProcedureAlreadyException(String procName) {
        super("Procedure " + procName + " already exist.");
    }
}
