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

import com.datawarehouse.be.pojo.dos.CustomWarningDO;

public interface CustomWarningService {

    /**
     * 添加报警信息
     * @param processDefinitionId
     * @param customWarningDO
     */
    void addCustomWarning(Long processDefinitionId, CustomWarningDO customWarningDO);

    /**
     * 删除报警信息
     * @param processDefinitionId
     */
    void deleteCustomWarning(Long processDefinitionId);

    /**
     * 获取报警信息
     * @param processDefinitionId
     * @return customWaningDO
     */
    CustomWarningDO getCustomWarningParam(Long processDefinitionId);

}
