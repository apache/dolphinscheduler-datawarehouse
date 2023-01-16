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

package com.datawarehouse.be.service;

import java.util.Map;

public interface HiveMetaService {

    /**
     * 分区名
     */
    String PARTITION = "PARTITION";

    /**
     * 分区格式
     */
    String FORMAT = "FORMAT";

    /**
     * 获取用户可查询库
     * @return
     */
    Map<String, Object> getHiveDbs();

    /**
     * 查询某个库下所有表
     * @param dbName
     * @return
     */
    Map<String, Object> getHiveTabsByDb(String dbName);

    /**
     * 通过关键词查询 hive table
     * @param keyword
     * @return
     */
    Map<String, Object> getHiveTabsByKeyword(String keyword);

    /**
     * 获取 hive table 信息：生存周期
     * @param dbName
     * @param tabName
     * @return
     */
    Map<String, Object> getHiveTabInfo(String dbName, String tabName);

    /**
     * 验证分区字段是否是 dt
     * @param dbName
     * @param tabName
     * @return
     */
    Map<String, Object> getPartitionName(String dbName, String tabName);

    /**
     *  获取字段信息
     * @param dbName
     * @param tabName
     * @return
     */
    Map<String, Object> getTableColumnList(String dbName, String tabName);

    /**
     * 验证表是否存在
     * @param dbName
     * @param tabName
     * @return
     */
    Map<String, Object> isTableExists(String dbName, String tabName);

    /**
     * 检查、获取表的分区
     * @param dbName
     * @param tabName
     */
    Map<String, String> checkPartition(String dbName, String tabName);
}
