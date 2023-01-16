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

package com.datawarehouse.be.service.impl;

import com.datawarehouse.be.service.SensitiveTableCheckService;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.common.utils.SqlParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class SensitiveTableCheckServiceImpl implements SensitiveTableCheckService {

    /**
     * 敏感库名
     */
    @Value("${hive.sensitive.dbName:ext}")
    private String sensitiveDbName;

    private static final Logger logger = LoggerFactory.getLogger(SensitiveTableCheckServiceImpl.class);


    @Override
    public void isTaskRightCreated(Map<String, Set<String>> ioMap) {
        /*
        1. 敏感表不可往外写
        2. 外表不可往敏感库写
        | 表/库           | 敏感库 | 非敏感库 |
        | 敏感表&&敏感表    |  √   | ×      |
        | 非敏感表&&敏感表  | √   | ×     |
        | 非敏感表&&非敏感表 | ×    | √      |
         */

        //获取输出库的库名
        Set<String> outputTables = ioMap.get(SqlParserUtil.OUT_PUT);
        Set<String> outputDbs = new HashSet<>();
        for (String outputTable : outputTables) {
            outputDbs.add(outputTable.toLowerCase().split("\\.")[0]);
        }

        //获取输入库的库名
        Set<String> inputTables = ioMap.get(SqlParserUtil.INPUT);
        Set<String> inputDbs = new HashSet<>();
        for (String inputTable : inputTables) {
            inputDbs.add(inputTable.toLowerCase().split("\\.")[0]);
        }

        // 敏感表写入敏感库
        if (inputDbs.contains(sensitiveDbName) && outputDbs.contains(sensitiveDbName)) {
            logger.info("敏感表 {} --> 写入敏感库:{}", String.join(",", inputTables), String.join(",", outputTables));
            return;
        }

        // 非敏感表写入非敏感库
        if (!inputDbs.contains(sensitiveDbName) && !outputDbs.contains(sensitiveDbName)) {
            logger.info("非敏感表 {} --> 写入非敏感库:{}",String.join(",",inputTables),String.join(",",outputTables));
            return; }

        // 其他情况，抛出异常
        logger.error("表 {} --> 写入:{}",String.join(",",inputTables),String.join(",",outputTables));
        throw new ServiceException("存在敏感库表往非敏感库表写入或非敏感库表往敏感库写入的情况");
    }


    @Override
    public void isCreatingSensitiveTable(String sql) {
        String regex = "create\\s+table\\s+(if\\s+not\\s+exists\\s+)?(.*?)\\(";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(sql.toLowerCase());
        while (matcher.find()) {
            String tableName = matcher.group(2);
            String dbName = tableName.split("\\.")[0];
            if (sensitiveDbName.equals(dbName)) {
                logger.error("存在创建敏感库表的create语句，{}", tableName);
                throw new ServiceException(String.format(String.format("建立敏感库：%s 中的表,请通过建表平台进行建立", dbName)));
            }
        }
    }
}