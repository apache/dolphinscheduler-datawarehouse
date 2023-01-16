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

/**
 * @AUTHOR :  wanglj9
 * @DATE :  2021/12/6 16:20
 */
public interface StaffInfoService {

    /**
     * 通过查询 hive 获取用户信息
     * @param keyword
     * @return
     */
    Map<String, Object> getStaffInfoByHive(String keyword);

    /**
     * 邮箱前缀模糊查询
     * @param keyword
     * @return
     */
    Map<String, Object> getStaffInfoByEmailPre(String keyword);

    /**
     * 获取 user 具有建表权限的 hive 库
     * @param userId
     * @return
     */
    Map<String, Object> getUserDepDatabases(Integer userId);

}
