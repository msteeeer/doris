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

suite("test_plsql_row_count") {
    def sourceTableName = "plsql_row_count_tbl_src"
    def resultT0 = "plsql_row_count_tbl_res_0"
    def resultT1 = "plsql_row_count_tbl_res_1"
    def procedureName = "plsql_row_count_proc";
    sql "DROP TABLE IF EXISTS ${sourceTableName}"
    sql "DROP TABLE IF EXISTS ${resultT0}"
    sql "DROP TABLE IF EXISTS ${resultT1}"
    sql """
        create table ${sourceTableName} (id int, name varchar(100)) UNIQUE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """
    sql """
        create table ${resultT0} (id BIGINT, row_count BIGINT, descript varchar(100)) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """
    sql """
        create table ${resultT1} (id int, name varchar(100)) UNIQUE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """
    sql """
        CREATE OR REPLACE PROCEDURE ${procedureName}() 
        BEGIN
            DECLARE v_id, v_res_id, v_index, v_count INT;
            DECLARE v_row_count BIGINT;
            DECLARE v_name STRING;
            DECLARE cur1 REF CURSOR;
            SET v_name = 'others';
            SET v_count = 100;
            WHILE v_index < v_count DO 
                INSERT INTO ${sourceTableName} values(v_index, v_name);
                SET v_index := v_index + 1;
            END WHILE;
            SET v_res_id = v_res_id + 1;
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            INSERT INTO ${resultT0} VALUES(v_res_id, v_row_count, "insert 1 row")

            SET v_res_id = v_res_id + 1;
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            INSERT INTO ${resultT0} VALUES(v_res_id, v_row_count, "insert 1 row")

            OPEN cur1 FOR select id, name from ${sourceTableName} order by id desc;
            set v_index = 0;
            WHILE v_index < v_count DO 
                FETCH cur1 INTO v_id, v_name;
                SET v_index := v_index + 1;
                if v_index = 10 OR v_index = 49 THEN
                    SET v_res_id = v_res_id + 1;
                    GET DIAGNOSTICS v_row_count = ROW_COUNT;
                    INSERT INTO ${resultT0} VALUES(v_res_id, v_row_count, "in query")
                END IF
            END WHILE;
            CLOSE cur1;
            
            OPEN cur1 FOR select id, name from ${sourceTableName} order by id desc;
            set v_index = 0;
            WHILE v_index < v_count DO 
                FETCH cur1 INTO v_id, v_name;
                SET v_index := v_index + 1;
            END WHILE;
            CLOSE cur1;
            SET v_res_id = v_res_id + 1;
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            INSERT INTO ${resultT0} VALUES (v_res_id, v_row_count, "cursor query 100 row")

            UPDATE ${sourceTableName} SET name = 'test1' WHERE id < 80;
            SET v_res_id = v_res_id + 1;
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            INSERT INTO ${resultT0} VALUES(v_res_id, v_row_count, "update 80 row");

            INSERT INTO ${sourceTableName} VALUES (1001, '1001 test'), (1002, '1002 test'),(1003, '1003 test');
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            SET v_res_id = v_res_id + 1;
            INSERT INTO ${resultT0} VALUES(v_res_id, v_row_count, "insert 3 row")
            
            INSERT INTO ${resultT1} SELECT * FROM ${sourceTableName} ;
            GET DIAGNOSTICS v_row_count = ROW_COUNT;
            SET v_res_id = v_res_id + 1;
            INSERT INTO ${resultT0} VALUES(v_res_id, v_row_count, "insert select 103 row")

        END;
        """
    sql """call ${procedureName}()"""
    qt_source """select * from ${sourceTableName} order by id"""
    qt_result_T0 """select * from ${resultT0} order by id"""
    qt_result_T1 """select * from ${resultT1} order by id desc"""
    sql """DROP PROCEDURE ${procedureName}"""
}
