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

import com.alibaba.fastjson.JSONObject;
import com.datawarehouse.be.pojo.dto.QueryDto;
import com.datawarehouse.be.service.VisualProcessService;
import org.apache.dolphinscheduler.common.enums.ProcessDefinitionType;
import org.apache.dolphinscheduler.dao.entity.ProcessCreateType;
import org.apache.dolphinscheduler.dao.mapper.ProcessCreateTypeMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class VisualProcessServiceImpl implements VisualProcessService {

    private static final Logger logger = LoggerFactory.getLogger(VisualProcessServiceImpl.class);

    @Autowired
    ProcessCreateTypeMapper processCreateTypeMapper;

    @Override
    public void addVisualQueryDto(QueryDto queryDto, Long processDefinitionId) {
        ProcessCreateType processCreateType;
        if (queryDto == null) {
            processCreateType = new ProcessCreateType(processDefinitionId, ProcessDefinitionType.NORMAL_PROCESS.getType(),null);
        } else {
            String  processDataStructure= JSONObject.toJSONString(queryDto);
            processCreateType = new ProcessCreateType(processDefinitionId, ProcessDefinitionType.VISUAL_PROCESS.getType(), processDataStructure);
        }

        processCreateTypeMapper.insertItem(processCreateType);
        logger.info("插入可视化 / 非可视化 sql建立工作流，构造 sql 请求参数");
    }

    @Override
    public void deleteVisualQueryDto(Long processDefinitionId) {
        processCreateTypeMapper.deleteItemByProcessID(processDefinitionId);
        logger.info("工作流 {} 的可视化/非可视信息请求参数 process_structure 已被删除");
    }

    @Override
    public QueryDto findQueryDtoById(Long processDefinitionId){
        ProcessCreateType processCreateType = processCreateTypeMapper.selectItemByProcessID(processDefinitionId);
        if (processCreateType != null) {
            String processDataStructure = processCreateType.getProcess_data_structure();
            QueryDto queryDto = JSONObject.parseObject(processDataStructure, QueryDto.class);
            return queryDto;
        }
        return null;
    }
}
