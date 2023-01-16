/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.api.strategy;

import org.apache.dolphinscheduler.common.enums.DbType;
import org.apache.dolphinscheduler.common.enums.IOType;
import org.apache.dolphinscheduler.common.enums.TaskType;
import org.apache.dolphinscheduler.common.model.TaskNode;

//import org.apache.dolphinscheduler.common.process.Property;
import org.apache.dolphinscheduler.common.task.sql.SqlBinds;
import org.apache.dolphinscheduler.common.task.sql.SqlParameters;
import org.apache.dolphinscheduler.common.utils.JSONUtils;

import org.apache.dolphinscheduler.spi.task.Property;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class SqlTaskStrategy implements ITaskStrategy {

    public abstract DbType dbType();

    @Override
    public TaskType taskType() {
        return TaskType.SQL;
    }

    public Map<IOType, List<String>> queryIOBySql(TaskNode taskNode) {
        SqlParameters sqlParameters = JSONUtils.parseObject(taskNode.getParams(), SqlParameters.class);
//        return queryIOBySqlParameters(sqlParameters);
        return queryIOBySql(sqlParameters.getSql());
    }

    public abstract Map<IOType, List<String>> queryIOBySqlParameters(SqlParameters sqlParameters);

    public abstract Map<IOType, List<String>> queryIOBySql(String sql);


    /**
     * 获取处理后的SQL和入参
     *
     * @param sqlParameters
     * @return
     */
    public List<SqlBinds> getSqlAndSqlParamsMap(SqlParameters sqlParameters) {
        return null;
//        return Optional.ofNullable(sqlParameters)
//                // 获取SQL
//                .map(SqlParameters::getSql)
//                // 按分号获取多个SQL
//                .map(e -> e.split(";"))
//                // 迭代处理多条SQl
//                .map(e -> Arrays.stream(e).map(sql -> {
//
//                    Map<Integer, Property> sqlParamsMap = new HashMap<>();
////                    StringBuilder sqlBuilder = new StringBuilder();
//
//                    //replace variable TIME with $[YYYYmmddd...] in sql
//                    Date nowDate = new Date();
//                    sql = ParameterUtils.replaceScheduleTime(sql, nowDate);
//
//                    Map<String, Property> paramsMap = sqlParameters.getLocalParametersMap();
//                    // spell SQL according to the final user-defined variable
////                    if (paramsMap == null) {
////                        sqlBuilder.append(sql);
////                    }
//
//                    if (StringUtils.isNotEmpty(sqlParameters.getTitle())) {
//                        String title = ParameterUtils.convertParameterPlaceholders(sqlParameters.getTitle(),
//                                ParamUtils.convert(paramsMap));
//                        sqlParameters.setTitle(title);
//                    }
//
//                    //new
//                    //replace variable TIME with $[YYYYmmddd...] in sql when history run job and batch complement job
//                    //sql = ParameterUtils.replaceScheduleTime(sql, taskExecutionContext.getScheduleTime());
//                    // special characters need to be escaped, ${} needs to be escaped
//                    String rgex = "['\"]*\\$\\{(.*?)\\}['\"]*";
//                    setSqlParamsMap(sql, rgex, sqlParamsMap, paramsMap);
//                    //Replace the original value in sql ！{...} ，Does not participate in precompilation
//                    String rgexo = "['\"]*\\!\\{(.*?)\\}['\"]*";
//                    sql = replaceOriginalValue(sql, rgexo, paramsMap);
//                    // replace the ${} of the SQL statement with the Placeholder
//                    sql = sql.replaceAll(rgex, "?");
////                    sqlBuilder.append(formatSql);
//
//                    return new SqlBinds(sql, sqlParamsMap);
//                }).collect(Collectors.toList())).orElse(null);
    }

    private void setSqlParamsMap(String content, String rgex, Map<Integer, Property> sqlParamsMap, Map<String, Property> paramsPropsMap) {
        Pattern pattern = Pattern.compile(rgex);
        Matcher m = pattern.matcher(content);
        int index = 1;
        while (m.find()) {

            String paramName = m.group(1);
            Property prop = paramsPropsMap.get(paramName);

            sqlParamsMap.put(index, prop);
            index++;
        }
    }

    private String replaceOriginalValue(String content, String rgex, Map<String, Property> sqlParamsMap) {
        Pattern pattern = Pattern.compile(rgex);
        while (true) {
            Matcher m = pattern.matcher(content);
            if (!m.find()) {
                break;
            }
            String paramName = m.group(1);
            String paramValue = sqlParamsMap.get(paramName).getValue();
            content = m.replaceFirst(paramValue);
        }
        return content;
    }

    private PreparedStatement prepareStatementAndBind(Connection connection, SqlBinds sqlBinds) throws Exception {
        PreparedStatement stmt = connection.prepareStatement(sqlBinds.getSql());
//        Map<Integer, Property> params = sqlBinds.getParamsMap();
        Map<Integer, Property> params = null;
        if (params != null) {
            for (Map.Entry<Integer, Property> entry : params.entrySet()) {
                Property prop = entry.getValue();
                //TODO:参数发生变化
                //ParameterUtils.setInParameter(entry.getKey(), stmt, prop.getType(), prop.getValue());
            }
        }
        return stmt;
    }

    public <T> T execute(Connection connection,
                         SqlBinds sqlBind, Function<ResultSet, T> rsResult) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = prepareStatementAndBind(connection, sqlBind);
            rs = pstmt.executeQuery();
            return rsResult.apply(rs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (Objects.nonNull(pstmt)) {
                    pstmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                if (Objects.nonNull(rs)) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
