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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Exec.java
// and modified by Doris

package org.apache.doris.plsql.executor;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;

import org.antlr.v4.runtime.ParserRuleContext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryErrorChecker {
    static final Pattern ERROR_MSG_PATTERN = Pattern.compile("errCode = ([0-9]+), detailMessage = (.*)");
    public static void checkAndThrowException(ConnectContext context, ParserRuleContext ctx, String sql) throws AnalysisException {
        if (context.getState().getStateType() == MysqlStateType.OK) {
            return;
        }
        StringBuilder errorMsg = new StringBuilder(1024);
        errorMsg.append("PL/SQL exception at line ");
        errorMsg.append(ctx.start.getLine());
        errorMsg.append(", reason: ");
        String detailErrorMessage = context.getState().getErrorMessage();
        Matcher matcher = ERROR_MSG_PATTERN.matcher(detailErrorMessage);
        if (matcher.find()) {
            errorMsg.append(matcher.group(2));
        } else {
            errorMsg.append(detailErrorMessage);
        }
        if (sql != null) {
           errorMsg.append(", error statement in PL/SQL: ");
           errorMsg.append(sql);
        }

        throw new AnalysisException(errorMsg.toString());
    }
}
