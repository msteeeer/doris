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

suite("test_plsql_expr_eval") {
    def createProcedure = { procedureName ,in_type_1, in_type_2 ->
        sql """
            CREATE OR REPLACE PROCEDURE ${procedureName}(op1 ${in_type_1}, op2 ${in_type_2}, op STRING, OUT res ${in_type_1})
            BEGIN
                DECLARE v_res ${in_type_1};
                IF op = '+' THEN
                    SET v_res = op1 + op2; 
                ELSEIF op = '-' THEN
                    SET v_res := op1 - op2;
                ELSEIF op = '*' THEN
                    SET v_res := op1 * op2;
                ELSEIF op = '/' THEN
                    SET v_res = op1 / op2;
                END IF
                SET res = v_res;
            END;
            """
    }
    def procedureName= 'plsql_expr_eval_proc';
    createProcedure(procedureName, 'INT', 'INT')
    sql """call ${procedureName}(100, 100, '+', @addVal)"""
    sql """call ${procedureName}(100, 100, '-', @subVal)"""
    sql """call ${procedureName}(100, 100, '*', @mulVal)"""
    sql """call ${procedureName}(100, 100, '/', @divVal)"""
    qt_int_op_int """select @addVal, @subVal, @mulVal, @divVal"""
    createProcedure(procedureName, 'DOUBLE', 'DOUBLE')
    sql """call ${procedureName}(23.76, 100.21, '+', @addVal)"""
    sql """call ${procedureName}(23.76, 100.21, '-', @subVal)"""
    sql """call ${procedureName}(23.76, 100.21, '*', @mulVal)"""
    sql """call ${procedureName}(23.76, 100.21, '/', @divVal)"""
    qt_double_op_double """select @addVal, @subVal, @mulVal, @divVal"""
    createProcedure(procedureName, 'DECIMAL(10,2)', 'DECIMAL(10,2)')
    sql """call ${procedureName}(23.76, 100.21, '+', @addVal)"""
    sql """call ${procedureName}(23.76, 100.21, '-', @subVal)"""
    sql """call ${procedureName}(23.76, 100.21, '*', @mulVal)"""
    sql """call ${procedureName}(23.76, 100.21, '/', @divVal)"""
    qt_decimal_op_decimal """select @addVal, @subVal, @mulVal, @divVal"""
    sql """call ${procedureName}(80, 20, '+', @addVal)"""
    sql """call ${procedureName}(80, 20, '-', @subVal)"""
    sql """call ${procedureName}(80, 20, '*', @mulVal)"""
    sql """call ${procedureName}(80, 20, '/', @divVal)"""
    qt_decimal_op_decimal """select @addVal, @subVal, @mulVal, @divVal"""
    createProcedure(procedureName, 'DECIMAL(10,2)', 'DOUBLE')
    sql """call ${procedureName}(23.76, 2.0, '+', @addVal)"""
    sql """call ${procedureName}(23.76, 2.0, '-', @subVal)"""
    sql """call ${procedureName}(23.76, 2.0, '*', @mulVal)"""
    sql """call ${procedureName}(23.76, 2.0, '/', @divVal)"""
    qt_decimal_op_double """select @addVal, @subVal, @mulVal, @divVal"""
    createProcedure(procedureName, 'DECIMAL(10,2)', 'INT')
    sql """call ${procedureName}(23.76, 2, '+', @addVal)"""
    sql """call ${procedureName}(23.76, 2, '-', @subVal)"""
    sql """call ${procedureName}(23.76, 2, '*', @mulVal)"""
    sql """call ${procedureName}(23.76, 2, '/', @divVal)"""
    qt_decimal_op_int """select @addVal, @subVal, @mulVal, @divVal"""
    createProcedure(procedureName, 'DOUBLE', 'INT')
    sql """call ${procedureName}(23.76, 2, '+', @addVal)"""
    sql """call ${procedureName}(23.76, 2, '-', @subVal)"""
    sql """call ${procedureName}(23.76, 2, '*', @mulVal)"""
    sql """call ${procedureName}(23.76, 2, '/', @divVal)"""
    qt_double_op_int """select @addVal, @subVal, @mulVal, @divVal"""
    sql """DROP PROCEDURE ${procedureName}"""
}
