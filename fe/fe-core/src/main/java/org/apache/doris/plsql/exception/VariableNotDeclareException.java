package org.apache.doris.plsql.exception;

import org.antlr.v4.runtime.ParserRuleContext;

public class VariableNotDeclareException extends RuntimeException{
    public VariableNotDeclareException(String ident) {
        super("Variable '" + ident + "' must be declared.");
    }
}
