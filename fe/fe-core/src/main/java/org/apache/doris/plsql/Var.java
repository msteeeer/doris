// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Var.java
// and modified by Doris

package org.apache.doris.plsql;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.httpv2.IllegalArgException;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.plsql.exception.ProcedureRuntimeException;
import org.apache.doris.plsql.exception.TypeException;
import org.apache.doris.plsql.executor.QueryResult;

import static org.apache.doris.catalog.Type.BIGINT;
import static org.apache.doris.catalog.Type.BOOLEAN;
import static org.apache.doris.catalog.Type.DATE;
import static org.apache.doris.catalog.Type.DATETIMEV2;
import static org.apache.doris.catalog.Type.DECIMAL128;
import static org.apache.doris.catalog.Type.DOUBLE;
import static org.apache.doris.catalog.Type.STRING;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Objects;

/**
 * Variable or the result of expression
 */
public class Var {
    // Data types
    public enum VarType {
        BOOL, CURSOR, DATE, DATETIME, DECIMAL, DERIVED_TYPE, DERIVED_ROWTYPE, DOUBLE, FILE, IDENT, BIGINT, INTERVAL, ROW,
        RS_LOCATOR, STRING, STRINGLIST, TIMESTAMP, NULL, PL_OBJECT
    }

    static Type getDorisType(VarType type) {
        switch (type) {
            case BOOL:
                return BOOLEAN;
            case DATE:
                return DATE;
            case DECIMAL:
                return DECIMAL128;
            case DOUBLE:
                return DOUBLE;
            case BIGINT:
                return BIGINT;
            case STRING:
                return STRING;
            case TIMESTAMP:
            case DATETIME:
                return DATETIMEV2;
            default:
                throw new IllegalArgumentException("Unsupported type mapping:" + type);
        }
    }

    public enum Operator {
        ADD, SUBTRACT, MULTIPLY, DIVIDE;
    }
    public static final String DERIVED_TYPE = "DERIVED%TYPE";
    public static final String DERIVED_ROWTYPE = "DERIVED%ROWTYPE";
    public static final Var EMPTY = new Var();
    public static final Var NULL = new Var(VarType.NULL);

    public String name;
    public VarType type;
    public Object value;

    int len;
    int scale;

    boolean constant = false;

    public Var() {
        type = VarType.NULL;
    }

    public Var(Var var) {
        name = var.name;
        type = var.type;
        value = var.value;
        len = var.len;
        scale = var.scale;
    }

    public Var(Long value) {
        this.type = VarType.BIGINT;
        this.value = value;
    }

    public Var(BigDecimal value) {
        this.type = VarType.DECIMAL;
        this.value = value;
    }

    public Var(String name, Long value) {
        this.type = VarType.BIGINT;
        this.name = name;
        this.value = value;
    }

    public Var(String value) {
        this.type = VarType.STRING;
        this.value = value;
    }

    public Var(Double value) {
        this.type = VarType.DOUBLE;
        this.value = value;
    }

    public Var(Date value) {
        this.type = VarType.DATE;
        this.value = value;
    }

    public Var(Timestamp value, int scale) {
        this.type = VarType.TIMESTAMP;
        this.value = value;
        this.scale = scale;
    }

    public Var(Interval value) {
        this.type = VarType.INTERVAL;
        this.value = value;
    }

    public Var(ArrayList<String> value) {
        this.type = VarType.STRINGLIST;
        this.value = value;
    }

    public Var(Boolean b) {
        type = VarType.BOOL;
        value = b;
    }

    public Var(String name, Row row) {
        this.name = name;
        this.type = VarType.ROW;
        this.value = new Row(row);
    }

    public Var(VarType type, String name) {
        this.type = type;
        this.name = name;
    }

    public Var(VarType type, Object value) {
        this.type = type;
        this.value = value;
    }

    public Var(String name, VarType type, Object value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public Var(VarType type) {
        this.type = type;
    }

    public Var(String name, String type, Integer len, Integer scale, Var def) {
        this.name = name;
        setType(type);
        if (len != null) {
            this.len = len;
        }
        if (scale != null) {
            this.scale = scale;
        }
        if (def != null) {
            cast(def);
        }
    }

    public Var(String name, String type, String len, String scale, Var def) {
        this(name, type, len != null ? Integer.parseInt(len) : null, scale != null ? Integer.parseInt(scale) : null,
                def);
    }

    /**
     * Cast a new value to the variable
     */
    public Var cast(Var val) {
        try {
            if (constant) {
                return this;
            } else if (val.value instanceof Literal) { // At first, ignore type
                value = val.value;
            } else if (val == null || val.value == null) {
                value = null;
            } else if (type == VarType.DERIVED_TYPE) {
                type = val.type;
                value = val.value;
            } else if (type == val.type && type == VarType.STRING) {
                cast((String) val.value);
            } else if (type == val.type) {
                value = val.value;
            } else if (type == VarType.STRING) {
                cast(val.toString());
            } else if (type == VarType.BIGINT) {
                if (val.type == VarType.STRING) {
                    value = Long.parseLong((String) val.value);
                } else if (val.type == VarType.DECIMAL) {
                    value = ((BigDecimal) val.value).longValue();
                } else if (val.type == VarType.BOOL) {
                    value = (Boolean) val.value ? 1 : 0;
                }
            } else if (type == VarType.DECIMAL) {
                if (val.type == VarType.STRING) {
                    value = new BigDecimal((String) val.value);
                } else if (val.type == VarType.BIGINT) {
                    value = BigDecimal.valueOf(val.longValue());
                } else if (val.type == VarType.DOUBLE) {
                    value = BigDecimal.valueOf(val.doubleValue());
                } else {
                    throw new TypeException(null, type, val.type, val.value);
                }
            } else if (type == VarType.DOUBLE) {
                if (val.type == VarType.STRING) {
                    value = Double.valueOf((String) val.value);
                } else if (val.type == VarType.BIGINT || val.type == VarType.DECIMAL) {
                    value = Double.valueOf(val.doubleValue());
                } else {
                    throw new TypeException(null, type, val.type, val.value);
                }
            } else if (type == VarType.DATE) {
                value = org.apache.doris.plsql.Utils.toDate(val.toString());
            } else if (type == VarType.TIMESTAMP || type == VarType.DATETIME) {
                value = org.apache.doris.plsql.Utils.toTimestamp(val.toString());
            }
        } catch (NumberFormatException e) {
            throw new TypeException(null, type, val.type, val.value);
        }
        return this;
    }

    public Literal toLiteral() {
        if (value instanceof Literal) {
            return (Literal) value;
        } else {
            return Literal.of(value);
        }
    }

    /**
     * Cast a new string value to the variable
     */
    public Var cast(String val) {
        if (!constant && type == VarType.STRING) {
            if (len != 0) {
                int l = val.length();
                if (l > len) {
                    value = val.substring(0, len);
                    return this;
                }
            }
            value = val;
        }
        return this;
    }

    /**
     * Set the new value
     */
    public void setValue(String str) {
        if (!constant && type == VarType.STRING) {
            value = str;
        }
    }

    public Var setValue(Long val) {
        if (!constant && type == VarType.BIGINT) {
            value = val;
        }
        return this;
    }

    public Var setValue(Boolean val) {
        if (!constant && type == VarType.BOOL) {
            value = val;
        }
        return this;
    }

    public void setValue(Object value) {
        if (!constant) {
            this.value = value;
        }
    }

    public Var setValue(QueryResult queryResult, int idx) throws AnalysisException {
        if (queryResult.jdbcType(idx) == Integer.MIN_VALUE) {
            value = queryResult.column(idx);
        } else { // JdbcQueryExecutor
            int type = queryResult.jdbcType(idx);
            if (type == java.sql.Types.CHAR || type == java.sql.Types.VARCHAR) {
                cast(new Var(queryResult.column(idx, String.class)));
            } else if (type == java.sql.Types.INTEGER || type == java.sql.Types.BIGINT
                    || type == java.sql.Types.SMALLINT || type == java.sql.Types.TINYINT) {
                cast(new Var(queryResult.column(idx, Long.class)));
            } else if (type == java.sql.Types.DECIMAL || type == java.sql.Types.NUMERIC) {
                cast(new Var(queryResult.column(idx, BigDecimal.class)));
            } else if (type == java.sql.Types.FLOAT || type == java.sql.Types.DOUBLE) {
                cast(new Var(queryResult.column(idx, Double.class)));
            }
        }
        return this;
    }

    public Var setRowValues(QueryResult queryResult) throws AnalysisException {
        Row row = (Row) this.value;
        int idx = 0;
        for (Column column : row.getColumns()) {
            Var var = new Var(column.getName(), column.getType(), (Integer) null, null, null);
            var.setValue(queryResult, idx);
            column.setValue(var);
            idx++;
        }
        return this;
    }

    /**
     * Set the data type from string representation
     */
    public void setType(String type) {
        this.type = defineType(type);
    }

    /**
     * Set the data type from JDBC type code
     */
    void setType(int type) {
        this.type = defineType(type);
    }

    /**
     * Set the variable as constant
     */
    void setConstant(boolean constant) {
        this.constant = constant;
    }

    /**
     * Define the data type from string representation
     * from hive type to plsql var type
     */
    public static VarType defineType(String type) {
        if (type == null) {
            return VarType.NULL;
        }  else if (type.equalsIgnoreCase("INT") || type.equalsIgnoreCase("INTEGER")
                || type.equalsIgnoreCase("BIGINT") || type.equalsIgnoreCase("SMALLINT")
                || type.equalsIgnoreCase("TINYINT") || type.equalsIgnoreCase("BINARY_INTEGER")
                || type.equalsIgnoreCase("PLS_INTEGER") || type.equalsIgnoreCase("SIMPLE_INTEGER")
                || type.equalsIgnoreCase("INT2") || type.equalsIgnoreCase("INT4")
                || type.equalsIgnoreCase("INT8")) {
            return VarType.BIGINT;
        }  else if (type.equalsIgnoreCase("CHAR") || type.equalsIgnoreCase("VARCHAR")
                || type.equalsIgnoreCase("VARCHAR2") || type.equalsIgnoreCase("TEXT")
                || type.equalsIgnoreCase("STRING") || type.equalsIgnoreCase("XML")
                || type.equalsIgnoreCase("CHARACTER")) {
            return VarType.STRING;
        } else if (type.equalsIgnoreCase("DEC") || type.equalsIgnoreCase("DECIMAL")
                || type.equalsIgnoreCase("NUMERIC") || type.equalsIgnoreCase("NUMBER")) {
            return VarType.DECIMAL;
        } else if (type.equalsIgnoreCase("REAL") || type.equalsIgnoreCase("FLOAT")
                || type.toUpperCase().startsWith("DOUBLE") || type.equalsIgnoreCase("BINARY_FLOAT")
                || type.toUpperCase().startsWith("BINARY_DOUBLE") || type.equalsIgnoreCase("SIMPLE_FLOAT")
                || type.toUpperCase().startsWith("SIMPLE_DOUBLE")) {
            return VarType.DOUBLE;
        } else if (type.equalsIgnoreCase("DATE")) {
            return VarType.DATE;
        } else if (type.equalsIgnoreCase("DATETIME")) {
            return VarType.DATETIME;
        }else if (type.equalsIgnoreCase("TIMESTAMP")) {
            return VarType.TIMESTAMP;
        } else if (type.equalsIgnoreCase("BOOL") || type.equalsIgnoreCase("BOOLEAN")) {
            return VarType.BOOL;
        } else if (type.equalsIgnoreCase("SYS_REFCURSOR")) {
            return VarType.CURSOR;
        } else if (type.equalsIgnoreCase("UTL_FILE.FILE_TYPE")) {
            return VarType.FILE;
        } else if (type.toUpperCase().startsWith("RESULT_SET_LOCATOR")) {
            return VarType.RS_LOCATOR;
        } else if (type.equalsIgnoreCase(Var.DERIVED_TYPE)) {
            return VarType.DERIVED_TYPE;
        } else if (type.equalsIgnoreCase(VarType.PL_OBJECT.name())) {
            return VarType.PL_OBJECT;
        } else if (type.equalsIgnoreCase(VarType.ROW.name())) {
            return VarType.ROW;
        }
        return VarType.NULL;
    }

    /**
     * Define the data type from JDBC type code
     */
    public static VarType defineType(int type) {
        if (type == java.sql.Types.CHAR || type == java.sql.Types.VARCHAR) {
            return VarType.STRING;
        } else if (type == java.sql.Types.INTEGER || type == java.sql.Types.BIGINT ) {
            return VarType.BIGINT;
        }
        return VarType.NULL;
    }

    /**
     * Remove value
     */
    public void removeValue() {
        type = VarType.NULL;
        name = null;
        value = null;
        len = 0;
        scale = 0;
    }

    /**
     * Compare values
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Var var = (Var) obj;

        if (!Objects.equals(var.type, type)) {
            return false;
        }

        if (!Objects.equals(var.value, value)) {
            return false;
        }
        if (!Objects.equals(var.name, name)) {
            return false;
        }

        if (!Objects.equals(var.len, len)) {
            return false;
        }

        if (!Objects.equals(var.scale, scale)) {
            return false;
        }

        if (!Objects.equals(var.constant, constant)) {
            return false;
        }
        return true;
    }

    /**
     * Check if variables of different data types are equal
     */
    public boolean equals(BigDecimal d, Long i) {
        return d.compareTo(new BigDecimal(i)) == 0;
    }

    /**
     * Compare values
     */
    public int compareTo(Var v) {
        try {
            return LiteralExpr.create(value.toString(), getDorisType(type)).compareTo(
                    LiteralExpr.create(v.value.toString(), getDorisType(v.type)));
        } catch (AnalysisException w) {
            throw new IllegalArgumentException("Failed to compare [" + value.toString() + ", " + v.value.toString()+ "]"
                    + " with type[" + type + "," + v.type + "]");
        }
    }

    /**
     * Calculate difference between values in percent
     */
    public BigDecimal percentDiff(Var var) {
        BigDecimal d1 = new Var(VarType.DECIMAL).cast(this).decimalValue();
        BigDecimal d2 = new Var(VarType.DECIMAL).cast(var).decimalValue();
        if (d1 != null && d2 != null) {
            if (d1.compareTo(BigDecimal.ZERO) != 0) {
                return d1.subtract(d2).abs().multiply(new BigDecimal(100)).divide(d1, 2, RoundingMode.HALF_UP);
            }
        }
        return null;
    }

    /**
     * Increment an integer value
     */
    public Var increment(long i) {
        if (type == VarType.BIGINT) {
            value = Long.valueOf(((Long) value).longValue() + i);
        }
        return this;
    }

    /**
     * Decrement an integer value
     */
    public Var decrement(long i) {
        if (type == VarType.BIGINT) {
            value = Long.valueOf(((Long) value).longValue() - i);
        }
        return this;
    }

    /**
     * Return an integer value
     */
    public int intValue() {
        if (type == VarType.BIGINT) {
            return ((Long) value).intValue();
        } else if (type == VarType.STRING) {
            return Integer.parseInt((String) value);
        }
        throw new NumberFormatException();
    }

    /**
     * Return a long integer value
     */
    public long longValue() {
        if (type == VarType.BIGINT) {
            return ((Number) value).longValue();
        }
        throw new NumberFormatException();
    }

    /**
     * Return a decimal value
     */
    public BigDecimal decimalValue() {
        if (type == VarType.DECIMAL) {
            return (BigDecimal) value;
        }
        throw new NumberFormatException();
    }

    /**
     * Return a double value
     */
    public double doubleValue() {
        if (type == VarType.DOUBLE || type == VarType.BIGINT || type == VarType.DECIMAL) {
            return ((Number) value).doubleValue();
        }
        throw new NumberFormatException();
    }

    /**
     * Return true/false for BOOL type
     */
    public boolean isTrue() {
        if (type != VarType.BIGINT && type != VarType.BOOL) {
            throw new IllegalArgumentException("isTrue should be applied to integer or bool type");
        }
        if (value == null) {
            return false;
        }
        if (type == VarType.BOOL) {
            return (Boolean) value;
        } else {
            return Long.parseLong(value.toString()) != 0;
        }
    }

    /**
     * Negate the value
     */
    public void negate() {
        if (value == null) {
            return;
        }
        if (type == VarType.BOOL) {
            boolean v = ((Boolean) value).booleanValue();
            value = Boolean.valueOf(!v);
        } else if (type == VarType.DECIMAL) {
            BigDecimal v = (BigDecimal) value;
            value = v.negate();
        } else if (type == VarType.DOUBLE) {
            Double v = (Double) value;
            value = -v;
        } else if (type == VarType.BIGINT) {
            Long v = (Long) value;
            value = -v;
        } else {
            throw new NumberFormatException("invalid type " + type);
        }
    }

    /**
     * Check if the variable contains NULL
     */
    public boolean isNull() {
        return type == VarType.NULL || value == null;
    }

    /**
     * Convert value to String
     */
    @Override
    public String toString() {
        if (value instanceof Literal) {
            return value.toString();
        } else if (value == null) {
            return null;
        } else if (type == VarType.BIGINT) {
            return String.valueOf(value);
        } else if (type == VarType.STRING) {
            return (String) value;
        } else if (type == VarType.DATE) {
            return ((Date) value).toString();
        } else if (type == VarType.TIMESTAMP || type == VarType.DATETIME) {
            int len = 19;
            String t = ((Timestamp) value).toString();   // .0 returned if the fractional part not set
            if (scale > 0) {
                len += scale + 1;
            }
            if (t.length() > len) {
                t = t.substring(0, len);
            }
            return t;
        }
        return value.toString();
    }

    /**
     * Convert value to SQL string - string literals are quoted and escaped, ab'c -&gt; 'ab''c'
     */
    public String toSqlString() {
        if (value == null) {
            return "NULL";
        } else if (type == VarType.STRING) {
            return org.apache.doris.plsql.Utils.quoteString((String) value);
        }
        return toString();
    }

    /**
     * Set variable name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get variable name
     */
    public String getName() {
        return name;
    }
}
