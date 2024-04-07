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

suite("test_plsql_exit_handler") {
    def diagnoseT0 = "plsql_exit_handler_tbl_diagnose_0"
    def resultT0 = "plsql_exit_handler_tbl_res_0"
    def procedureName = "plsql_exit_handler_proc";
    sql "DROP TABLE IF EXISTS ${resultT0}"
    sql "DROP TABLE IF EXISTS ${diagnoseT0}"
    sql """
        create table ${resultT0} (id int, name varchar(20)) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
        properties ("replication_num"="1");
        """
    sql """
        create table ${diagnoseT0} (sql_code int, msg varchar(1000), error_scope varchar(1000)) DUPLICATE key(`sql_code`) distributed by hash (`sql_code`) buckets 3
        properties ("replication_num"="1");
        """
    sql """
        CREATE OR REPLACE PROCEDURE ${procedureName}() 
        BEGIN
            DECLARE v_msg STRING;
            DECLARE v_sqlcode INT;
            BEGIN
                DECLARE EXIT HANDLER FOR SQLEXCEPTION 
                    BEGIN
                        GET DIAGNOSTICS CONDITION 1
                        v_msg = MESSAGE_TEXT, v_sqlcode = GBASE_ERRNO;
                        insert into ${diagnoseT0} values (v_sqlcode, v_msg, 'test_0_outer');
                    END
                BEGIN
                    DECLARE EXIT HANDLER FOR SQLEXCEPTION 
                        BEGIN
                            GET DIAGNOSTICS CONDITION 1
                            v_msg = MESSAGE_TEXT, v_sqlcode = GBASE_ERRNO;
                            insert into ${diagnoseT0} values (v_sqlcode, v_msg, 'test_0_inner');
                        END
                    insert into ${resultT0} values(1, "plsql111"); -- expected execution
                    select * from fake_table;
                END
                select fake_column from ${resultT0};
                insert into ${resultT0} values(1111111, "exception");
            END
            BEGIN
                BEGIN
                     DECLARE EXIT HANDLER FOR SQLSTATE '04000'
                        BEGIN
                            GET DIAGNOSTICS CONDITION 1
                            v_msg = MESSAGE_TEXT, v_sqlcode = MYSQL_ERRNO;
                            insert into ${diagnoseT0} values (v_sqlcode, v_msg, 'test_1_outer');
                        END
                     BEGIN
                        BEGIN
                            BEGIN
                                select * from fake_table;
                            END
                            insert into ${resultT0} values(2222222, "exception");
                        END
                     END
                     insert into ${resultT0} values(3333333, "exception");
                END
                insert into ${resultT0} values(2, "plsql222"); -- expected

            END
            BEGIN
                DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
                    BEGIN
                        GET DIAGNOSTICS CONDITION 1
                        v_msg = MESSAGE_TEXT, v_sqlcode = GBASE_ERRNO;
                        insert into ${diagnoseT0} values (v_sqlcode, v_msg, 'test_2_outer');
                    END
                BEGIN
                    select * from fake_table;
                    insert into ${resultT0} values(3, "plsql333"); -- expected
                END
            END
        END
        """
    sql """call ${procedureName}()"""
    qt_diagnose_t0 "select * from ${diagnoseT0} order by error_scope, sql_code, msg"
    qt_result_t0 "select * from ${resultT0} order by id"
    sql """DROP PROCEDURE ${procedureName}"""
}
