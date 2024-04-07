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

suite("test_plsql_loop") {
    def sourceTableName = "plsql_loop_tbl_src"
    def resultT0 = "plsql_loop_tbl_res_0"
    def procedureName = "plsql_loop_proc";
    sql "DROP TABLE IF EXISTS ${sourceTableName}"
    sql "DROP TABLE IF EXISTS ${resultT0}"
    sql """
        create table ${sourceTableName} (id int, name varchar(20)) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """
    sql """
        create table ${resultT0} (id int, name varchar(20)) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """

    sql """INSERT INTO ${sourceTableName} VALUES (1, "plsql111")"""
    sql """INSERT INTO ${sourceTableName} VALUES (2, "plsql222")"""
    sql """INSERT INTO ${sourceTableName} VALUES (3, "plsql333")"""
    sql """INSERT INTO ${sourceTableName} VALUES (4, "plsql444")"""
    sql """INSERT INTO ${sourceTableName} VALUES (5, "plsql555")"""
    sql """
        CREATE OR REPLACE PROCEDURE ${procedureName}() 
        BEGIN
            DECLARE v_id, v_index INT;
            DECLARE v_name STRING;
            DECLARE cur1 REF CURSOR;
            DECLARE sql_text TEXT;
            DECLARE done INT;
            DECLARE state STRING;
   
            SET sql_text = 'select id, name from ${sourceTableName} order by id';
            OPEN cur1 FOR select id, name from ${sourceTableName} order by id desc;
            level_0: LOOP
                FETCH cur1 INTO v_id, v_name;
                SET done = SQLCODE;
                INSERT INTO ${resultT0} values(v_id, @v_name);
                IF done != 0 THEN
                    LEAVE level_0;
                END IF
            END LOOP;
            CLOSE cur1;
            
            OPEN cur1 FOR @sql_text;
            level_0: LOOP
                FETCH cur1 INTO v_id, v_name;
                SET state = SQLSTATE;
                SET v_id = v_id * 1000;
                SET v_index = 0;
                level_1: LOOP
                    INSERT INTO ${resultT0} values(@v_id + v_index, v_name);   
                    IF v_index > 2 THEN
                        LEAVE level_1
                    END IF
                    SET v_index = v_index + 1;
                END LOOP;
                IF state = '020000' THEN 
                    LEAVE level_0;
                END IF
            END LOOP level_0;
            CLOSE cur1;
            
            OPEN cur1 FOR @sql_text;
            level_0: LOOP
                FETCH cur1 INTO v_id, v_name;
                SET v_id = v_id * 1000000;
                SET v_index = 0;
                level_1: LOOP
                    IF v_name = 'plsql444' THEN
                        LEAVE level_0
                    END IF
                    INSERT INTO ${resultT0} values(@v_id + @v_index, v_name);
                    IF v_index > 2 THEN
                        LEAVE level_1;
                    END IF;
                    SET v_index = v_index +1;
                END LOOP;
            END LOOP level_0;
            CLOSE cur1;
        END;
        """
    sql """call ${procedureName}()"""
    qt_result_T0 """select * from ${resultT0} order by id"""
    sql """DROP PROCEDURE ${procedureName}"""
}
