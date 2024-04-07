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

suite("test_plsql_repeat") {
    def sourceTableName = "plsql_repeat_tbl_src"
    def resultT0 = "plsql_repeat_tbl_res_0"
    def resultT1 = "plsql_repeat_tbl_res_1"
    def procedureName = "plsql_repeat_proc";
    sql "DROP TABLE IF EXISTS ${sourceTableName}"
    sql "DROP TABLE IF EXISTS ${resultT0}"
    sql "DROP TABLE IF EXISTS ${resultT1}"
    sql """
        create table ${sourceTableName} (id int, name varchar(20)) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """
    sql """
        create table ${resultT0} (id int, name varchar(20)) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """
    sql """
        create table ${resultT1} (id int, name varchar(20)) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """
    sql """INSERT INTO ${sourceTableName} VALUES (111, "plsql111")"""
    sql """INSERT INTO ${sourceTableName} VALUES (222, "plsql222")"""
    sql """INSERT INTO ${sourceTableName} VALUES (333, "plsql333")"""
    sql """INSERT INTO ${sourceTableName} VALUES (444, "plsql444")"""
    sql """INSERT INTO ${sourceTableName} VALUES (555, "plsql555")"""
    sql """
        CREATE OR REPLACE PROCEDURE ${procedureName}() 
        BEGIN
        DECLARE v_index, v_row_count INT DEFAULT 0;
        DECLARE v_name STRING;
        SELECT count(1) INTO v_row_count from ${sourceTableName};
        DECLARE cur1 CURSOR FOR select name from ${sourceTableName} order by id asc;
        SET v_index = 0;
        OPEN cur1;
        REPEAT
            FETCH cur1 INTO v_name;
            INSERT INTO ${resultT0} values(@v_index, v_name);
            COMMIT;
            SET v_index = v_index + 1;
        UNTIL v_index >= v_row_count END REPEAT;
        CLOSE cur1;
        
        DECLARE cur2 CURSOR FOR select name from ${sourceTableName} order by id desc;
        SET v_index = 0;
        OPEN cur2;
        WHILE v_index < v_row_count DO 
            FETCH cur2 INTO v_name;
            INSERT INTO ${resultT1} values(v_index, v_name);
            SET v_index := v_index + 1;
        END WHILE;
        CLOSE cur1;
        
        END;
        """
    sql """call ${procedureName}()"""
    qt_result_T0 """select * from ${resultT0} order by id"""
    qt_result_T1 """select * from ${resultT1} order by id"""
    sql """DROP PROCEDURE ${procedureName}"""
}
