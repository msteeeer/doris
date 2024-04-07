package org.apache.doris.plsql;

public enum SqlState {
    SUCCESS("000000"),
    WARNING("01000"),
    NOT_FOUND("020000"),
    EXCEPTION("040000");
    private String stateCode;
    SqlState(String stateCode) {
        this.stateCode = stateCode;
    }

    public String getStateCode() {
        return stateCode;
    }

    @Override
    public String toString() {
        return stateCode;
    }
}
