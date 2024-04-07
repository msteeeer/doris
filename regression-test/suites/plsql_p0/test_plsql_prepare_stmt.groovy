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

suite("test_plsql_prepare_stmt") {
    def procedureName = "plsql_prepare_stmt_proc";
    def resultT0 = "plsql_prepare_stmt_tbl_res_0"
    sql "DROP TABLE IF EXISTS ${resultT0}"
    sql """
        create table ${resultT0} (id int, name varchar(20)) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """
    sql """
        CREATE OR REPLACE PROCEDURE ${procedureName}() 
        BEGIN
            DECLARE v_sqlstr TEXT;
            SET @v_sqlstr = 'INSERT INTO ${resultT0} VALUES (111, "plsql111")'
            PREPARE exec_plsql_stmt FROM @v_sqlstr;
            EXECUTE exec_plsql_stmt;
            DROP PREPARE exec_plsql_stmt;
            PREPARE exec_plsql_stmt FROM 'INSERT INTO ${resultT0} VALUES (222, "plsql222")';
            EXECUTE exec_plsql_stmt;
            DEALLOCATE PREPARE exec_plsql_stmt;
        END
        """
    sql """call ${procedureName}()"""
    qt_result_t0 """select * from ${resultT0}"""
    sql """DROP PROCEDURE ${procedureName}"""
}
