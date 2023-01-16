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

import org.apache.dolphinscheduler.dao.entity.User;

import java.util.Map;

public interface ExportDataService {
    /**
     * query export data work process instance list by dbName,tableName
     * @param dbName
     * @param tableName
     * @param startTimeFilterVal
     * @param pageNo
     * @param pageSize
     * @return
     */
    public Map<String, Object> queryExportDataProcessInstanceList(
            String dbName,
            String tableName,
            String startTimeFilterVal,
            Integer pageNo,
            Integer pageSize);

    /**
     * get table details:last update time,partition Days
     * @param dbName
     * @param tableName
     * @param dataSourceName
     * @return
     */
    public Map<String, Object>  getTableDetails(String dbName, String tableName, String dataSourceName);

    /**
     * get process definition id
     * @param dbName
     * @param tableName
     * @return
     */
    public Map<String, Object> getProcessId(String dbName, String tableName);


    /**
     * get all dt tables on db
     * @param dbName
     * @return
     */
    public Map<String, Object> getDTHiveTable(String dbName);

    /**
     * Fuzzy query to obtain table information based on keyword
     *
     * @param keyword
     * @return
     */
    Map<String, Object> getTabsByFuzzyQuery(String keyword);

    /**
     * Verify that the user is the owner of the db.table
     * @param loginUser
     * @param dbName
     * @param tableName
     * @return
     */
    Map<String, Object> verifyPermission(User loginUser, String dbName, String tableName);
}
