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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.dolphinscheduler.common.enums.DbType;
import org.apache.dolphinscheduler.common.enums.IOType;
import org.apache.dolphinscheduler.common.task.sql.SqlBinds;
import org.apache.dolphinscheduler.common.task.sql.SqlParameters;
import org.apache.dolphinscheduler.common.utils.SqlParserUtil;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class HiveSqlTaskStrategy extends SqlTaskStrategy {

    @Autowired
    private ProcessService processService;


    @Override
    public DbType dbType() {
        return DbType.HIVE;
    }

    @Override
    public Map<IOType, List<String>> queryIOBySql(String sql) {
        // 获取sql子句
        if (StringUtils.isBlank(sql)) {
            return null;
        }

        // 获取输入输出表
        Map<String, Set<String>> hiveIOResult = SqlParserUtil.getIOOfSql(sql, true);
        if (MapUtils.isEmpty(hiveIOResult)) {
            return null;
        }
        Map<IOType, List<String>> result = new HashMap();
        result.put(IOType.INPUT, Lists.newArrayList(hiveIOResult.get(SqlParserUtil.INPUT)));
        result.put(IOType.OUTPUT, Lists.newArrayList(hiveIOResult.get(SqlParserUtil.OUT_PUT)));
        return result;
    }

    @Override
    public Map<IOType, List<String>> queryIOBySqlParameters(SqlParameters sqlParameters) {
        // exclude env setting of sql
        excludeEnvSettingOfSql(sqlParameters);

        Map<IOType, List<String>> result = new HashMap();

//        DataSource datasource = processService.findDataSourceById(sqlParameters.getDatasource());
//        BaseConnectionParam baseConnectionParam = (BaseConnectionParam) DatasourceUtil.buildConnectionParams(
//                DbType.valueOf(sqlParameters.getType()),
//                datasource.getConnectionParams());
//        Connection connection = DatasourceUtil.getConnection(DbType.valueOf(sqlParameters.getType()), baseConnectionParam);

        try {
//            connection = DatasourceUtil.getConnection(DbType.valueOf(sqlParameters.getType()), baseConnectionParam);
            List<SqlBinds> sqlBindsList = getSqlAndSqlParamsMap(sqlParameters);
            if (CollectionUtils.isEmpty(sqlBindsList)) {
                return result;
            }
            for (SqlBinds sqlBinds : sqlBindsList) {

//                sqlBinds = new SqlBinds("EXPLAIN FORMATTED AUTHORIZATION \n" + sqlBinds.getSql(), sqlBinds.getParamsMap());
//                HiveIOResult hiveIOResult = execute(connection, sqlBinds, rs -> {
//                    try {
//                        rs.next();
//                        return JSONUtils.parseObject(rs.getString("Explain"), HiveIOResult.class);
//                    } catch (SQLException e) {
//                        throw new RuntimeException("获取explain结果失败！", e);
//                    }
//                });
//                result.put(IOType.INPUT, excludeSpecificIOItems(hiveIOResult.getInputs()));
//                result.put(IOType.OUTPUT, excludeSpecificIOItems(hiveIOResult.getOutputs()));

                Map<String, Set<String>> hiveIOResult = SqlParserUtil.getIOOfSql(sqlBinds.getSql(), true);
                if (MapUtils.isEmpty(hiveIOResult)) {
                    continue;
                }
                result.put(IOType.INPUT, Lists.newArrayList(hiveIOResult.get(SqlParserUtil.INPUT)));
                result.put(IOType.OUTPUT, Lists.newArrayList(hiveIOResult.get(SqlParserUtil.OUT_PUT)));
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("解析SQL中输入输出表方法异常，详情：%s", e.toString()));
        }

        return result;
    }

    /**
     * 排除特定的输入输出项
     *
     * @param hiveIOList
     * @return
     */
    private static List<String> excludeSpecificIOItems(List<String> hiveIOList) {
        /*
            业务逻辑：
                正常的输入输出格式:dbName@tableName@dt=20210902，故排除IO主要分为：
                    1.临时表(hdfs://zrHdfsHa/data/kafkaData/ods_im/ods_im_conversation_user)开头
                    2.带分区的表(dwd@dwd_cust_appoint_f_d@dt=20210801)
         */
        if (CollectionUtils.isEmpty(hiveIOList)) {
            return hiveIOList;
        }
        List<String> list = Lists.newArrayList();
        hiveIOList.forEach(e -> {
            if (StringUtils.startsWithIgnoreCase(e, "hdfs://")) {
                return;
            }
            String[] ioArray = e.split("@");
            int length = ioArray.length;
            String ioStr = null;
            if (length == 2) {
                // 标准格式(dwd@dwd_cust_appoint_f_d)，直接添加
                ioStr = e.replace("@", ".");
            } else if (length > 2) {
                // 带分区的表(dwd@dwd_cust_appoint_f_d@dt=20210801)，只添加库和表
                ioStr = new StringBuffer(ioArray[0]).append(".").append(ioArray[1]).toString();
            }

            // 若不存在，则添加
            if (ioStr != null && !list.contains(ioStr)) {
                list.add(ioStr);
            }
        });
        return list;
    }

    /**
     * 排除sql语句中环境变量设置
     *
     * @param sqlParameters
     */
    public void excludeEnvSettingOfSql(SqlParameters sqlParameters) {
        // verify sql
        String sql = sqlParameters.getSql();
        if (StringUtils.isBlank(sql)) {
            return;
        }

        // escape hive env settingcommont of sql
        List<String> sqlList = SqlParserUtil.splitSql(sql);
        // wrap sql
        StringBuffer finalSql = new StringBuffer();
        if (CollectionUtils.isNotEmpty(sqlList)) {
            for (int i = 0, j = sqlList.size(); i < j; i++) {
                String item = sqlList.get(i);
                if (StringUtils.isBlank(item)) {
                    continue;
                }
                finalSql.append(item);
                if (i + 1 < j) {
                    finalSql.append(";");
                }
            }
        }
        sqlParameters.setSql(finalSql.toString());

//        String[] sqlArray = sql.split(";");
//        if (sqlArray.length > 2) {
//            // new sql
//            StringBuffer newSql = new StringBuffer();
//
//            // 2个及以上的sql，类似：
//            // CREATE TABLE IF NOT EXISTS test.dwd_find_house_cycle_f_d(a String,b String,c String);
//            // set hive.tez.container.size=8192;
//            // set hive.vectorized.execution.enabled=false;
//            // INSERT OVERWRITE TABLE test.dwd_find_house_cycle_f_d partition(dt='$[yyyyMMdd-1]') select a,b,c from test;
//
//            // 获取sql类型，ddl:create;dml:insert,select,update,delete;env_config:set
//            for (int i = 0, sqlArrayLength = sqlArray.length; i < sqlArrayLength; i++) {
//                String e = sqlArray[i];
//                if (StringUtils.isBlank(e)) {
//                    continue;
//                }
//                String sqlItem = e.trim();
//                boolean isEnvConfig = org.springframework.util.StringUtils.startsWithIgnoreCase(sqlItem, "set");
//
//                // 设置任务中前置SQL，SQL，后置SQL
//                if (!isEnvConfig) {
//                    // 最后sql语句不加分号，否则有语法错误
//                    if (i < sqlArrayLength - 1) {
//                        e += ";";
//                    }
//                    newSql.append(e);
//                }
//            }
//            sqlParameters.setSql(newSql.toString());
//        }
    }


}
