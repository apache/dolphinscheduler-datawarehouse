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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.datawarehouse.be.configuration.SensorConfig;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import com.datawarehouse.be.service.ProcessTikService;
import org.apache.dolphinscheduler.common.enums.Priority;

import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ProcessKitServiceImpl implements ProcessTikService {

    private static final Logger logger = LoggerFactory.getLogger(ProcessKitServiceImpl.class);

    @Autowired
    private ProcessDefinitionMapper processDefinitionMapper;

    @Autowired
    private SensorConfig sensorConfig;

    @Override
    public void changeSensorProcessPriority() {
        ProcessDefinition processDefinition = processDefinitionMapper.queryByDefineId(sensorConfig.processId);
        String processJson = "processDefinition.getProcessDefinitionJson()";
        logger.info("改前ProcessJson：{}", processJson);
        JSONObject processJsonObject = JSON.parseObject(processJson);
        JSONArray tasks = processJsonObject.getJSONArray("tasks");
        if (tasks != null && tasks.size() > 0) {
            for (Object task : tasks) {
                JSONObject taskJsonObject = (JSONObject) task;
                taskJsonObject.put("taskInstancePriority", Priority.HIGHEST.toString());
            }
            processJsonObject.put("tasks", tasks);
            //processDefinition.setProcessDefinitionJson(processJsonObject.toJSONString());
        }
        logger.info("改后ProcessJson：{}", tasks);
        // 更新工作流
        processDefinitionMapper.updateById(processDefinition);
    }

    @Transactional(rollbackFor = Exception.class)
    @Override
    public void fixSensorProcessJson() {
        // 获取探测分区任务工作流
        ProcessDefinition processDefinition = processDefinitionMapper.queryByDefineId(sensorConfig.processId);

        // 获取工作流名称
        String processJson = "processDefinition.getProcessDefinitionJson()";
        logger.info("改前ProcessJson：{}", processJson);
        JSONObject processJsonObject = JSON.parseObject(processJson);
        JSONArray tasks = processJsonObject.getJSONArray("tasks");
        JSONArray newTasks = new JSONArray();
        if (tasks != null && tasks.size() > 0) {
            for (Object task : tasks) {
                JSONObject taskJsonObject = (JSONObject) task;

                String taskName = taskJsonObject.getString("name");
                String[] taskNameArray = taskName.split("\\.");
                String dbName = taskNameArray[0];
                String tableName = taskNameArray[1];

                JSONObject params = taskJsonObject.getJSONObject("params");
                String rawScript = params.getString("rawScript");
                String[] rawScriptArray = rawScript.substring(rawScript.indexOf("(") + 1).split(",");
                String targetDBName = rawScriptArray[0];
                String targetTableName = rawScriptArray[1];
                rawScript = rawScript.replace(targetDBName, "'" + dbName + "'");
                rawScript = rawScript.replace(targetTableName, "'" + tableName + "'");

                params.put("rawScript", rawScript);
                taskJsonObject.put("params", params);

                newTasks.add(taskJsonObject);
            }
            processJsonObject.put("tasks", newTasks);
            //processDefinition.setProcessDefinitionJson(processJsonObject.toJSONString());
        }
        logger.info("改后ProcessJson：{}", newTasks);

        // 获取工作流定义
        processDefinitionMapper.updateById(processDefinition);
    }

}
