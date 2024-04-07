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

suite("test_plsql_type") {
    def sourceTableName = "plsql_type_tbl_src"
    def resultT0 = "plsql_type_tbl_res_0"
    def procedureNameP0 = "plsql_type_proc_0";
    def procedureNameP1 = "plsql_type_proc_1";
    def procedureNameP2 = "plsql_type_proc_2";
    def procedureNameP3 = "plsql_type_proc_3";
    sql "DROP TABLE IF EXISTS ${sourceTableName}"
    sql "DROP TABLE IF EXISTS ${resultT0}"
    sql """
        CREATE TABLE ${sourceTableName} (
            `id` int(11) NULL,
            `user_name` varchar(20) NULL,
            `order_date` DATE,
            `order_datetime` DATETIME,
            `price` DECIMAL(18,5),
            `descript` STRING,
            `comment` TEXT
        )
        ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        CREATE TABLE ${resultT0} (
            `id` int(11) NULL,
            `user_name` varchar(20) NULL,
            `order_date` DATE,
            `order_datetime` DATETIME,
            `price` DECIMAL(18,5),
            `descript` STRING,
            `comment` TEXT
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        CREATE OR REPLACE PROCEDURE ${procedureNameP0}(arg_id INT, IN arg_user_name VARCHAR(20), IN arg_order_date DATE,
                 IN arg_order_datetime DATETIME, IN arg_price DECIMAL(10,2), arg_descript STRING, arg_comment TEXT)
        BEGIN
            INSERT INTO ${sourceTableName} VALUES (
                arg_id, arg_user_name, arg_order_date, arg_order_datetime, arg_price, arg_descript, arg_comment
             );
        END;
        """
    sql """
        CALL ${procedureNameP0}(1, 'plsql111', '2024-01-01', '2024-01-01 01:01:02', 5203.5132,'normal','test test_plsql_type 000');
        """
    sql """
        CALL ${procedureNameP0}(2, 'plsql222', '2024-01-02', '2024-01-02 12:58:02', 15203.23, 'abnormal', 'test test_plsql_type 111');
        """
    sql """
        CALL ${procedureNameP0}(3, 'plsql333', '2024-01-03', '2024-01-03 22:59:04', 1523.32132, 'unknown','test test_plsql_type 222');
        """
    sql """
        CREATE OR REPLACE PROCEDURE ${procedureNameP1}(OUT out_id INT, INOUT out_user_name VARCHAR(20), OUT out_order_date DATE,
                 OUT out_order_datetime DATETIME, OUT out_price DECIMAL(10,2), 
                 OUT out_descript STRING, OUT out_comment TEXT)
        BEGIN
            DECLARE v_id INT;
            DECLARE v_user_name varchar(20);
            DECLARE v_order_date DATE;
            DECLARE v_order_datetime DATETIME;
            DECLARE v_price DECIMAL(10,2);
            DECLARE v_descript STRING;
            DECLARE v_comment TEXT;

            DECLARE done INT;
            SET done = 0;
            DECLARE cur1 CURSOR FOR SELECT * FROM ${sourceTableName} ORDER BY ID DESC;
            OPEN cur1;
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
            WHILE NOT done DO
                FETCH cur1 INTO v_id, v_user_name, v_order_date, v_order_datetime, v_price, v_descript, v_comment;
                INSERT INTO ${resultT0} values(v_id, v_user_name, v_order_date, v_order_datetime, v_price, v_descript, v_comment);
            END WHILE
            CLOSE cur1;
            SET out_id = v_id;
            SET out_user_name = v_user_name;
            SET out_order_date = v_order_date;
            SET out_order_datetime = v_order_datetime;
            SET out_price = v_price;
            SET out_descript = v_descript;
            SET out_comment = v_comment;
        END;
        """
    sql """
        CREATE OR REPLACE PROCEDURE ${procedureNameP2}(OUT out_id INT, INOUT out_user_name VARCHAR(20), OUT out_order_date DATE,
                 OUT out_order_datetime DATETIME, OUT out_price DECIMAL(10,2), 
                 OUT out_descript STRING, OUT out_comment TEXT)
        BEGIN
            SET out_id = 1002;
            SET out_user_name = 'procedure name';
            SET out_order_date = '2024-12-12';
            SET out_order_datetime = '2024-12-12 00:03:04';
            SET out_price = 1234567890123.12345;
            SET out_descript = 'no descript';
            SET out_comment = "no comment";
        END;
        """
    sql """
        CREATE OR REPLACE PROCEDURE ${procedureNameP3}(OUT out_id INT, INOUT out_user_name VARCHAR(20), OUT out_order_date DATE,
                 OUT out_order_datetime DATETIME, OUT out_price DECIMAL(10,2), 
                 OUT out_descript STRING, OUT out_comment TEXT)
        BEGIN
            SELECT id, user_name, order_date, order_datetime, price, descript, comment 
            INTO out_id, out_user_name, out_order_date, out_order_datetime, out_price, out_descript,out_comment
            FROM ${sourceTableName}
            WHERE id = 1;
        END;
        """
    sql """CALL ${procedureNameP1}(@id, @user_name, @order_date, @order_datetime, @price, @descript, @comment)"""
    qt_variable """SELECT @id, @user_name, @order_date, @order_datetime, @price"""
    qt_descript """SELECT @descript"""
    qt_comment """SELECT @comment"""
    qt_result """SELECT * FROM ${resultT0} ORDER BY id"""
    sql """CALL ${procedureNameP2}(@id, @user_name, @order_date, @order_datetime, @price, @descript, @comment)"""
    qt_variable """SELECT @id, @user_name, @order_date, @order_datetime, @price"""
    qt_descript """SELECT @descript"""
    qt_comment """SELECT @comment"""
    sql """CALL ${procedureNameP3}(@id, @user_name, @order_date, @order_datetime, @price, @descript, @comment)"""
    qt_variable """SELECT @id, @user_name, @order_date, @order_datetime, @price"""
    qt_descript """SELECT @descript"""
    qt_comment """SELECT @comment"""
    sql """DROP PROCEDURE ${procedureNameP0}"""
    sql """DROP PROCEDURE ${procedureNameP1}"""
    sql """DROP PROCEDURE ${procedureNameP2}"""
    sql """DROP PROCEDURE ${procedureNameP3}"""
}
