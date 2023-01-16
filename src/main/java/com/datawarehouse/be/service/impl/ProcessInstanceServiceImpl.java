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

import static org.apache.dolphinscheduler.common.Constants.DATA_LIST;

import com.google.common.collect.Maps;
import com.datawarehouse.be.configuration.SensorConfig;
import com.datawarehouse.be.constants.ProcessConstant;
import com.datawarehouse.be.enums.ExportTypeEnums;
import org.apache.dolphinscheduler.api.service.ProcessDefinitionService;
import org.apache.dolphinscheduler.common.enums.IOType;
import com.datawarehouse.be.enums.ExecutionStatusEnums;
import com.datawarehouse.be.pojo.dos.CustomProcessInstanceDO;
import com.datawarehouse.be.pojo.vo.ProcessInstanceDependencyVO;
import com.datawarehouse.be.pojo.vo.ProcessOutputVO;
import com.datawarehouse.be.service.ProcessInstanceService;
import org.apache.dolphinscheduler.api.enums.Status;

import org.apache.dolphinscheduler.api.service.UsersService;
import org.apache.dolphinscheduler.api.service.impl.BaseServiceImpl;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.DependentRelation;
import org.apache.dolphinscheduler.common.enums.Flag;

import org.apache.dolphinscheduler.common.model.DateInterval;
import org.apache.dolphinscheduler.common.model.DependentItem;
import org.apache.dolphinscheduler.common.model.DependentTaskModel;
import org.apache.dolphinscheduler.common.task.dependent.DependentParameters;
import org.apache.dolphinscheduler.common.task.sql.SqlParameters;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.DependentUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;

import org.apache.dolphinscheduler.dao.entity.*;
import org.apache.dolphinscheduler.dao.mapper.*;

import org.apache.dolphinscheduler.server.utils.DependentExecute;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ProcessInstanceServiceImpl extends BaseServiceImpl implements ProcessInstanceService {

    @Autowired
    private ProcessDefinitionMapper processDefinitionMapper;

    @Autowired
    private ProcessDefinitionIOMapper processDefinitionIOMapper;

    @Autowired
    private TaskInstanceMapper taskInstanceMapper;

    @Autowired
    private UsersService usersService;

    @Autowired
    private ProcessService processService;

    @Autowired
    private ZROutPutMapper outPutMapper;

    @Autowired
    private SensorConfig sensorConfig;

    @Autowired
    private ProcessDefinitionService processDefinitionService;

    @Autowired
    private TaskInstanceMapper taskInstanceMapper;

    @Override
    public Map<String, Object> queryDependencyByInstanceId(String projectName, Integer processInstanceId) {
        Map<String, Object> result = new HashMap<>();
        List<ProcessInstanceDependencyVO> dependentProcessInstance = new ArrayList<ProcessInstanceDependencyVO>();
        ProcessInstance processInstanceDetail = processService.findProcessInstanceDetailById(processInstanceId);
        Date currentTime;
        if(processInstanceDetail.getScheduleTime() != null){
            currentTime = processInstanceDetail.getScheduleTime();
        }else{
            currentTime = processInstanceDetail.getStartTime();
        }
        List<TaskInstance> validTaskListByProcessId = processService.findValidTaskListByProcessId(processInstanceId);
        Date finalCurrentTime = currentTime;
        validTaskListByProcessId.stream()
                .filter(x->x.getTaskType().equals("DEPENDENT"))
            .forEach(taskInstance -> {
                    DependentParameters dependentParameters = taskInstance.getDependency();
                    DependentRelation relation = dependentParameters.getRelation();
                    List<DependentTaskModel> dependTaskList = dependentParameters.getDependTaskList();
                    dependTaskList.stream().forEach(dependTask->{
                        List<DependentItem> dependItemList = dependTask.getDependItemList();
                        DependentExecute dependentExecute = new DependentExecute(dependItemList, relation);
                        dependItemList.stream().forEach(dependentItem->{
                            List<DateInterval> dateIntervals = DependentUtils.getDateIntervalList(finalCurrentTime, dependentItem.getDateValue());
                            for(int i=dateIntervals.size();i > 0; i--){
                                ProcessInstance dependProcessInstance = dependentExecute.findLastProcessInterval(dependentItem.getDefinitionCode(),dateIntervals.get(i-1));

                                if(dependProcessInstance != null){
                                    ProcessInstanceDependencyVO processInstanceDependencyVO = new ProcessInstanceDependencyVO();
                                    processInstanceDependencyVO.setCurTaskInstanceId(taskInstance.getId());
                                    processInstanceDependencyVO.setDependentProcessId(dependProcessInstance.getProcessDefinitionCode());
                                    processInstanceDependencyVO.setDependentProcessInstanceId(dependProcessInstance.getId());
                                    processInstanceDependencyVO.setDependentProcessInstanceName(dependProcessInstance.getName());
                                    processInstanceDependencyVO.setStartTime(dependProcessInstance.getStartTime());
                                    processInstanceDependencyVO.setEndTime(dependProcessInstance.getEndTime());
                                    ExecutionStatusEnums ProcessStatus = ExecutionStatusEnums.getZiroomTaskStatus(dependProcessInstance.getState());
                                    processInstanceDependencyVO.setStatus(ProcessStatus.getDescp());
                                    processInstanceDependencyVO.setDependentTaskType(Constants.DEPENDENT_ALL);
                                    ProcessDefinition processDefinition = processDefinitionMapper.queryByDefineId(dependProcessInstance.getProcessDefinitionCode());
                                    String processDefinitionName = processDefinition.getName();
                                    processInstanceDependencyVO.setDependentProcessName(processDefinitionName);
                                    // 根据ID，获取用户名
                                    if(dependProcessInstance.getExecutorId() > 0){
                                        User user = usersService.queryUser(processDefinition.getUserId());
                                        processInstanceDependencyVO.setExecutorName(user.getUserName());
                                    }
                                    if (processDefinitionName.contains("sensor")){
                                        String[] split = processDefinitionName.split("\\.");
                                        processInstanceDependencyVO.setTableName(split[0] + "." + split[1]);
                                    }else {
                                        processInstanceDependencyVO.setTableName(processDefinitionName);
                                    }
                                    dependentProcessInstance.add(processInstanceDependencyVO);
                                    break;
                                }
                            }
                        });
                    });
                });
        result.put(Constants.DATA_LIST, dependentProcessInstance);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public Map<String, Object> queryCustomProcessInstanceById(String projectName, Integer processInstanceId) {
        Map<String, Object> result = new HashMap<>();
        ProcessInstance processInstance = processService.findProcessInstanceDetailById(processInstanceId);
        processInstance.setDuration(DateUtils.format2Duration(processInstance.getStartTime(), processInstance.getEndTime()));
        ProcessDefinition processDefinition = processDefinitionMapper.queryByCode(processInstance.getProcessDefinitionCode());
        List<ProcessTaskRelation> processTaskRelations = processService.findRelationByCode(processDefinition.getCode(), processDefinition.getVersion());
        List<TaskDefinitionLog> taskDefinitionLogs = processService.getTaskDefineLogListByRelation(processTaskRelations);
        TaskDefinitionLog sqlinstance = taskDefinitionLogs.stream().filter(x -> x.getTaskType().equals("SQL")).findFirst().get();
        SqlParameters sqlParameters = JSONUtils.parseObject(sqlinstance.getTaskParams(), SqlParameters.class);
        String sql = sqlParameters.getSql();
        CustomProcessInstanceDO customProcessInstanceDO = new CustomProcessInstanceDO();
        customProcessInstanceDO.setDuration(DateUtils.format2Duration(processInstance.getStartTime(), processInstance.getEndTime()));
        customProcessInstanceDO.setProcessInstanceId(processInstance.getId());
        customProcessInstanceDO.setProcessName(processDefinition.getName());
        customProcessInstanceDO.setProcessInstanceName(processInstance.getName());
        customProcessInstanceDO.setCreateTime(processInstance.getStartTime());
        customProcessInstanceDO.setEndTime(processInstance.getEndTime());
        customProcessInstanceDO.setDescription(processDefinition.getDescription());
        customProcessInstanceDO.setProjectId(processDefinition.getProjectCode());
        customProcessInstanceDO.setProjectId(processDefinition.getProjectCode());
        customProcessInstanceDO.setProjectName(projectName);
        customProcessInstanceDO.setSql(sql);
        customProcessInstanceDO.setExecutorName(usersService.queryUser(processInstance.getExecutorId()).getUserName());
        result.put(DATA_LIST, customProcessInstanceDO);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public Map<String, Object> queryOutputByInstanceId(String projectName, Integer processInstanceId) {
        Map<String, Object> result = new HashMap<>();
        List<ProcessOutputVO> processOutputVOList = new ArrayList<ProcessOutputVO>();
        List<TaskInstance> taskInstances = taskInstanceMapper.findValidTaskListByProcessId(processInstanceId,Flag.YES);
        ProcessInstance processInstance = processService.findProcessInstanceDetailById(processInstanceId);
        Map<String, TaskInstance> taskInstanceMap = new HashMap<String, TaskInstance>();
        taskInstances.stream().forEach(taskInstance->{
            taskInstanceMap.put(taskInstance.getName(), taskInstance);
        });
        /** 查询产出表信息 */
        List<ProcessDefinitionIO> processDefinitionIOS = processDefinitionIOMapper
            .queryByProcessDefinitionIdAndIoType(processInstance.getProcessDefinitionCode(), IOType.OUTPUT.getCode());
        /** 查询产出的hive表信息 */
        processDefinitionIOS.stream().forEach(processDefinitionIO->{
            ProcessOutputVO processOutputVO = new ProcessOutputVO();
            String ioName = processDefinitionIO.getIoName();
            String[] split = ioName.split("\\.");
            String databaseName = split[0];
            String tableName = split[1];
            String taskName = processDefinitionIO.getTaskName();
            TaskInstance taskInstance = taskInstanceMap.get(taskName);
            if(taskInstance != null){
                processOutputVO.setTaskInstanceId(taskInstance.getId());
                processOutputVO.setOutPutType(ExportTypeEnums.HIVE.getDescp());
                processOutputVO.setOutputDatabaseName(ExportTypeEnums.HIVE.getDescp() + "：" + databaseName);
                processOutputVO.setOutputTableName(tableName);
                processOutputVO.setEndTime(taskInstance.getEndTime());
                ExecutionStatusEnums ziroomTaskStatus = ExecutionStatusEnums.getZiroomTaskStatus(taskInstance.getState());
                processOutputVO.setStatus(ziroomTaskStatus.getDescp());
                processOutputVOList.add(processOutputVO);
            }
        });

        /** 查询导出到ch */
        taskInstanceMap.entrySet().stream()
            .filter(item-> ProcessConstant.EXPORT_TASK_NAME.equals(item.getKey()))
            .forEach(taskInstance->{
                TaskInstance value = taskInstance.getValue();
                ProcessInstance instance = processService.findProcessInstanceDetailById(value.getProcessInstanceId());
                long processDefinitionId = instance.getProcessDefinitionCode();
                Map<String, Object> conditionParam = Maps.newHashMapWithExpectedSize(1);
                conditionParam.put("process_id", processDefinitionId);
                List<OutPutEntity> outPutEntities = outPutMapper.selectByMap(conditionParam);
                outPutEntities.stream().forEach(outPutEntitie->{
                    ProcessOutputVO processOutputVO = new ProcessOutputVO();
                    String dbName = outPutEntitie.getDbName();
                    String tableName = outPutEntitie.getTableName();
                    processOutputVO.setTaskInstanceId(value.getId());
                    processOutputVO.setOutPutType(ExportTypeEnums.CHICKHOUSE.getDescp());
                    processOutputVO.setOutputDatabaseName(ExportTypeEnums.CHICKHOUSE.getDescp() + "：" + dbName);
                    processOutputVO.setOutputTableName(tableName);
                    processOutputVO.setEndTime(value.getEndTime());
                    ExecutionStatusEnums ziroomTaskStatus = ExecutionStatusEnums.getZiroomTaskStatus(value.getState());
                    processOutputVO.setStatus(ziroomTaskStatus.getDescp());
                    processOutputVOList.add(processOutputVO);
                });

            });
        putMsg(result, Status.SUCCESS);
        result.put(DATA_LIST, processOutputVOList);
        return result;
    }
}
