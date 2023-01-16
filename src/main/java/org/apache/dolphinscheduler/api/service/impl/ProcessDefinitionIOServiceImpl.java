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

package org.apache.dolphinscheduler.api.service.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;

import org.apache.dolphinscheduler.api.service.ProcessDefinitionIOService;
import org.apache.dolphinscheduler.api.service.TaskStrategyService;
import org.apache.dolphinscheduler.common.enums.IOType;
import org.apache.dolphinscheduler.dao.entity.*;

import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionIOMapper;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class ProcessDefinitionIOServiceImpl extends BaseServiceImpl implements ProcessDefinitionIOService {

    @Autowired
    private TaskStrategyService taskStrategyService;

    @Autowired
    private ProcessDefinitionIOMapper processDefinitionIOMapper;

    @Autowired
    private ProcessDefinitionMapper processDefinitionMapper;

    @Autowired
    private ProcessService processService;

    /**
     * 根据processDefinitionId查询出processDefinition
     * 解析出所有的task content，如果task类型为sql类型，那么计算此
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void saveProcessDefinitionIO(ProcessDefinition processDefinition) {
        long processDefinitionCode = processDefinition.getCode();
        List<ProcessTaskRelation> processTaskRelations = processService.findRelationByCode(processDefinitionCode, processDefinition.getVersion());
        List<TaskDefinitionLog> taskDefinitionLogs = processService.getTaskDefineLogListByRelation(processTaskRelations);
        Map<IOType, Set<TaskIODO>> ioMap = processService.transformTask(processTaskRelations,taskDefinitionLogs).stream()
                // 获取每个任务输入输出集合,并转换为TaskIODO对象，方便获取Task信息
                .map(item -> {
                            Map<IOType, List<String>> map = taskStrategyService.queryIOByTaskNode(item);
                            // 过滤空输入输出
                            if (map == null || map.isEmpty()) {
                                return null;
                            }
                            Map<IOType, List<TaskIODO>> taskIOMap = Maps.newHashMap();
                            map.forEach((k, v) -> {
                                taskIOMap.put(k, v.stream().map(e -> new TaskIODO(item.getId(), item.getName(), e)).collect(Collectors.toList()));
                            });
                            return taskIOMap;
                        }
                )
                // 过滤空输入输出
                .filter(e -> e != null)
                // 任务集合查询到的多个输入输出Map，合并为一个Map
                .flatMap(item -> item.entrySet().stream())
                .flatMap(item -> item.getValue().stream().collect(Collectors.toMap(v -> v, v -> item.getKey(), (k1, k2) -> k2)).entrySet().stream())
                .collect(Collectors.groupingBy(item -> item.getValue(), Collectors.mapping(item -> item.getKey(), Collectors.toSet())));


        //去重，去掉上游是自己产出的部分
        Set<TaskIODO> outputs = ioMap.getOrDefault(IOType.OUTPUT, Collections.emptySet());
        Set<TaskIODO> inputs = ioMap.getOrDefault(IOType.INPUT, Collections.emptySet());
        inputs.stream().filter(item -> outputs.contains(item)).forEach(item -> inputs.remove(item));
        // 判断是否添加血缘
        if (!weatherAddLineage(outputs, processDefinitionCode)) {
            // 血缘已存在，不添加
            return;
        }

        processDefinitionIOMapper.deleteByDefineId(processDefinitionCode);
        //mybatis plus暂时没有批量插入的接口，先单条写入
        ioMap.entrySet().stream()
                .flatMap(item -> item.getValue().stream().map(v -> new ProcessDefinitionIO(processDefinitionCode, processDefinition.getVersion(), item.getKey(), v.getTableName(), processDefinition.getName(), v.getTaskId(), v.getTaskName())))
                .forEach(item -> processDefinitionIOMapper.insert(item));
    }

    /**
     * 是否添加血缘
     */
    private boolean weatherAddLineage(Set<TaskIODO> outputs,Long processId){
        if (CollectionUtils.isEmpty(outputs)) {
            return false;
        }
        // 结果，默认添加血缘
        boolean result = true;

        // 输出表只会存在一个，在新增修改工作流方法中有校验
        String tableName = Lists.newArrayList(outputs).get(0).getTableName();
        List<ProcessDefinitionIO> processDefinitionIOS = processDefinitionIOMapper.queryByIOName(tableName);

        if (CollectionUtils.isNotEmpty(processDefinitionIOS)) {
            // 修改场景，如果修改后的输出表和之前一样，不添加，反之添加血缘
            ProcessDefinitionIO processDefinitionIO = processDefinitionIOS.stream().filter(e -> e.getProcessDefinitionCode() == processId.intValue()).findFirst().orElse(null);
            if (processDefinitionIO != null) {
                // 存在输出表，不添加血缘
                result = false;
            }
        }
        return result;
    }

    public void deleteProcessDefinitionIO(Long processDefinitionCode) {
        processDefinitionIOMapper.deleteByDefineId(processDefinitionCode);
    }

    @Data
    public static class TaskIODO {
        /**
         * task node id
         */
        private String taskId;

        /**
         * task node name
         */
        private String taskName;

        /**
         * table of io
         */
        private String tableName;

        /**
         * 排重,基于编号
         *
         * @return
         */
        @Override
        public int hashCode() {
            return Objects.hashCode(this.tableName);
        }

        /**
         * 排重,基于编号
         *
         * @param obj
         * @return
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof TaskIODO)) {
                return false;
            }
            return Objects.equals(this.tableName, ((TaskIODO) obj).getTableName());
        }

        public TaskIODO() {
        }

        public TaskIODO(String taskId, String taskName, String tableName) {
            this.taskId = taskId;
            this.taskName = taskName;
            this.tableName = tableName;
        }
    }
}

