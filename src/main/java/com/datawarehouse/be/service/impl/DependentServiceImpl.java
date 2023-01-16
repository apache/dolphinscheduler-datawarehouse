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

import com.datawarehouse.be.configuration.SensorConfig;
import com.datawarehouse.be.exceptions.DependentServiceException;
import com.datawarehouse.be.service.DependentService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DependentServiceImpl implements DependentService {

    @Autowired
    private SensorConfig sensorConfig;

    @Autowired
    private ProcessDefinitionMapper processDefinitionMapper;

    private String wrapDepName(String tableName, String tablePartition) {
        return new StringBuffer(tableName).append(".").append(tablePartition).append(".sensor").toString();
    }

    @Override
    public List<ProcessDefinition> getDependentTasks(Set<String> tableNames, String tablePartition) {
        if (CollectionUtils.isEmpty(tableNames)) {
            return null;
        }
        try {
            List<String> wrapTableNames = tableNames.stream().map(e -> wrapDepName(e, tablePartition)).collect(Collectors.toList());
            return processDefinitionMapper.queryByDefineNames(sensorConfig.processId, wrapTableNames);
        } catch (Exception e) {
            throw new DependentServiceException(String.format("获取common项目中工作流任务依赖异常，入参tableNames:%s，tablePartition:%s, 详情：%s", tableNames, tablePartition, e.toString()));
        }
    }
}
