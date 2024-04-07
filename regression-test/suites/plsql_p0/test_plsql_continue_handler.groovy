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

suite("test_plsql_continue_handler") {
    def sourceTableName = "plsql_continue_handler_tbl_src"
    def resultT0 = "plsql_continue_handler_tbl_res_0"
    def procedureName = "psql_continue_handler_proc";
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
    sql """INSERT INTO ${sourceTableName} VALUES (111, "plsql111")"""
    sql """INSERT INTO ${sourceTableName} VALUES (222, "plsql222")"""
    sql """INSERT INTO ${sourceTableName} VALUES (333, "plsql333")"""
    sql """INSERT INTO ${sourceTableName} VALUES (444, "plsql444")"""
    sql """INSERT INTO ${sourceTableName} VALUES (555, "plsql555")"""
    sql """
        CREATE OR REPLACE PROCEDURE ${procedureName}() 
        BEGIN
            DECLARE v_index, v_row_count, v_id INT DEFAULT (0);
            DECLARE v_name STRING;
            DECLARE v_inner_done INT DEFAULT FALSE;
            DECLARE v_outer_done BOOL;
            BEGIN
                DECLARE CONTINUE HANDLER FOR SQLSTATE '020000' SET v_inner_done = TRUE;
                DECLARE cur1 CURSOR FOR select id, name from ${sourceTableName} order by id;
                SET v_index = 0;
                OPEN cur1;
                level_1: LOOP
                    FETCH cur1 INTO v_id, v_name;
                    if v_inner_done then
                        leave level_1;
                    end if;
                    SET v_index = v_index+1;
                    INSERT INTO ${resultT0} values(v_index, v_name);
                END LOOP level_1;
                CLOSE cur1;
            END
            BEGIN
                SET v_inner_done = 0;
                DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_inner_done = 1;
                DECLARE cur2 CURSOR FOR select id, name from ${sourceTableName} order by id;
                SET v_index = 1000;
                OPEN cur2;
                level_1: LOOP
                    FETCH cur2 INTO v_id, v_name;
                    if v_inner_done then
                        leave level_1;
                    end if;
                    SET v_index = v_index + 1;
                    INSERT INTO ${resultT0} values(v_index, v_name);
                END LOOP level_1;
                CLOSE cur1;
            END
            BEGIN
                SET v_inner_done = 0;
                SET v_outer_done = false;
                DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_outer_done = true;
                DECLARE cur2 CURSOR FOR select id, name from ${sourceTableName} order by id;

                OPEN cur2;
                WHILE NOT v_outer_done DO
                    FETCH cur2 INTO v_id, v_name;
                    SET v_index = v_id * 1000000;
                    if NOT v_outer_done then
                        DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_inner_done = 1;
                        DECLARE cur3 CURSOR FOR SELECT name FROM ${sourceTableName} order by id;
                        OPEN cur3;
                        SET v_inner_done = 0;
                        WHILE NOT v_inner_done DO
                            FETCH cur3 INTO v_name;
                            IF NOT v_inner_done THEN
                                SET v_index = v_index + 1;
                                INSERT INTO ${resultT0} values(v_index, v_name);
                            END IF
                        END WHILE;
                        CLOSE cur3; 
                    END IF
                END WHILE;
                CLOSE cur1;
            END
        END
        """
    sql """call ${procedureName}()"""
    qt_select "select * from ${resultT0} order by id"
    sql """DROP PROCEDURE ${procedureName}"""
}
