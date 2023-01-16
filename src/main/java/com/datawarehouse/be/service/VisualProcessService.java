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

import com.datawarehouse.be.pojo.dto.QueryDto;

public interface VisualProcessService {

    /**
     * 可视化拼接参数
     * @param queryDto
     * @param processDefinitionId
     */
    void addVisualQueryDto(QueryDto queryDto, Long processDefinitionId);

    /**
     * 工作流 id
     * @param processDefinitionId
     * @return
     */
    void deleteVisualQueryDto(Long processDefinitionId);

    /**
     * 根据工作流 id 获取相应可视化参数构建对象
     * @param processDefinitionId
     * @return
     */
    QueryDto findQueryDtoById(Long processDefinitionId);
}