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
import com.google.common.collect.Maps;
import com.datawarehouse.be.configuration.SensorConfig;

import com.datawarehouse.be.constants.ProcessConstant;

import com.datawarehouse.be.exceptions.ProcessConvertException;
import com.datawarehouse.be.exceptions.ProcessParamsException;
import com.datawarehouse.be.pojo.dos.DepDO;
import com.datawarehouse.be.pojo.dto.ExportDto;
import com.datawarehouse.be.pojo.params.AddEditProcessParam;
import com.datawarehouse.be.pojo.params.AirflowDAGAddParam;
import com.datawarehouse.be.pojo.vo.ProcessDetailVO;
import com.datawarehouse.be.pojo.vo.SqlParsingVO;
import com.datawarehouse.be.service.DagConverterService;
import com.datawarehouse.be.service.ExportDataProcessService;
import com.datawarehouse.be.service.ExportDataService;
import com.datawarehouse.be.service.ProcessService;
import com.datawarehouse.be.service.TaskService;
import com.datawarehouse.be.utils.ClickHouseUtil;
import org.apache.dolphinscheduler.api.enums.Status;

import org.apache.dolphinscheduler.api.service.ExecutorService;
import org.apache.dolphinscheduler.api.service.impl.BaseServiceImpl;
import org.apache.dolphinscheduler.common.Constants;

import org.apache.dolphinscheduler.common.enums.FailureStrategy;
import org.apache.dolphinscheduler.common.enums.IOType;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.RunMode;
import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.common.utils.CollectionUtils;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.entity.UserGroup;
import org.apache.dolphinscheduler.dao.entity.DepEntity;
import org.apache.dolphinscheduler.dao.entity.OutPutEntity;
import org.apache.dolphinscheduler.dao.entity.OutPutExportEntity;
import org.apache.dolphinscheduler.dao.entity.ScheduleEntity;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinitionIO;
import org.apache.dolphinscheduler.dao.mapper.ExportDataMapper;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.apache.dolphinscheduler.dao.mapper.UserGroupMapper;
import org.apache.dolphinscheduler.dao.mapper.UserMapper;
import org.apache.dolphinscheduler.dao.mapper.ZRDepMapper;
import org.apache.dolphinscheduler.dao.mapper.ZROutPutExportMapper;
import org.apache.dolphinscheduler.dao.mapper.ZROutPutMapper;
import org.apache.dolphinscheduler.dao.mapper.ZRScheduleMapper;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionIOMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Service("exportDataProcessService")
public class ExportDataProcessServiceImpl extends BaseServiceImpl implements ExportDataProcessService {
    private static final Logger logger = LoggerFactory.getLogger(ExportDataProcessServiceImpl.class);

    @Autowired
    ProcessDefinitionIOMapper processDefinitionIOMapper;

    @Autowired
    ZROutPutExportMapper outPutExportMapper;

    @Autowired
    ZROutPutMapper outPutMapper;

    @Autowired
    ExportDataMapper exportDataMapper;

    @Autowired
    UserGroupMapper userGroupMapper;

    @Autowired
    ProjectMapper projectMapper;

    @Autowired
    ZRDepMapper zrDepMapper;

    @Autowired
    ZRScheduleMapper zrScheduleMapper;

    @Autowired
    UserMapper userMapper;

    @Autowired
    ProcessService processService;

    @Autowired
    SensorConfig sensorConfig;

    @Autowired
    ExecutorService execService;

    @Autowired
    DagConverterService dagConverterService;

    @Autowired
    ExportDataService exportDataService;

    @Autowired
    ProcessServiceImpl processServiceImpl;
    @Autowired
    private TaskService taskService;
    @Autowired
    ClickHouseUtil clickHouseUtil;

    @Transactional(rollbackFor = Exception.class)
    @Override
    public void addEditExportDataProcess(User loginUser, ExportDto exportDto) throws Exception {
        /**
         * 1、先判断是否存在工作流，并构建AddEditProcessParam
         *      （1）不存在，则新建一个由依赖和导出构成的工作流
         *      （2）存在工作流a：
         *                 【1】该工作流a不存在导出任务，新建一个仅有导出构成的工作流b，依赖已有的a工作流
         *                 【2】该工作流a存在导出任务，则修改之前的工作流，仅修改partitiondays
         */
        exportDto.verify();
        Map<String, Object> dataMap = exportDataService.getProcessId(exportDto.getDbName(), exportDto.getTableName());
        Map<String, Object> processIdMap = (Map) dataMap.get(Constants.DATA_LIST);
        Long processDefinitionCode = (Long) processIdMap.get("processDefinitionId");
        if (processDefinitionCode == null) {
            AddEditProcessParam notExistsParam = getNotExistsParam(loginUser, null, exportDto.getDbName(), exportDto.getTableName(), exportDto.getDataSourceName(), exportDto.getPartitionDays());
            addEditExportProcess(loginUser, notExistsParam, exportDto.getDbName(), exportDto.getTableName());
        } else {
            Integer whetherExport = exportDataMapper.checkWhetherExport(processDefinitionCode);
            if (whetherExport == 0) {
                AddEditProcessParam notDSExistsParam = getNotExistsParam(loginUser, null, exportDto.getDbName(), exportDto.getTableName(), exportDto.getDataSourceName(), exportDto.getPartitionDays());
                addEditExportProcess(loginUser, notDSExistsParam, exportDto.getDbName(), exportDto.getTableName());
            } else {
                List<ProcessDefinitionIO> processDefinitionIOS = processDefinitionIOMapper.queryByProcessDefinitionIdAndIoType(processDefinitionCode, IOType.OUTPUT.getCode());
                if (processDefinitionIOS == null || processDefinitionIOS.size() == 0) {
                    AddEditProcessParam notDSExistsParam = getNotExistsParam(loginUser, processDefinitionCode, exportDto.getDbName(), exportDto.getTableName(), exportDto.getDataSourceName(), exportDto.getPartitionDays());
                    addEditExportProcess(loginUser, notDSExistsParam, exportDto.getDbName(), exportDto.getTableName());
                } else {
                    AddEditProcessParam addEditProcessParam = getExportTaskExistsParam(loginUser, processDefinitionCode, exportDto.getDataSourceName(), exportDto.getPartitionDays());
                    processService.addEditProcess(addEditProcessParam, loginUser);
                    startExportProcess(loginUser, addEditProcessParam.getProjectName(), addEditProcessParam.getProcessId());
                }
            }
        }
    }

    /**
     * start export process
     *
     * @param loginUser
     * @param projectName
     * @param processId
     */
    private void startExportProcess(User loginUser, String projectName, Long processId) {
        FailureStrategy failureStrategy = FailureStrategy.END;
        WarningType warningType = WarningType.NONE;
        TaskDependType taskDependType = TaskDependType.TASK_POST;
        RunMode runMode = RunMode.RUN_MODE_SERIAL;
        Priority processInstancePriority = Priority.MEDIUM;

        int warningGroupId = 0;
        String workerGroup = "default";
        String startNodeList = null;
        String receivers = null;
        String receiversCc = null;
        Integer timeout = null;

        //检查自任务
        //TODO:方法发生变化
        //Map<String, Object> checkResult = execService.startCheckByProcessDefinedId(processId);
        Map<String, Object> checkResult = null;
        if (checkResult != null && checkResult.containsKey(Constants.STATUS) && !checkResult.get(Constants.STATUS).equals(Status.SUCCESS)) {
            return;
        }

        if (timeout == null) {
            timeout = Constants.MAX_TASK_TIMEOUT;
        }

        try {
            //TODO:方法发生变化
//            Map<String, Object> result = execService.execProcessInstance(loginUser, projectName, processId, null, CommandType.START_PROCESS, failureStrategy,
//                    startNodeList, taskDependType, warningType,
//                    warningGroupId, receivers, receiversCc, runMode, processInstancePriority, workerGroup, timeout);
            Map<String, Object> result = null;
        } catch (Exception e) {
            logger.info(e.getMessage());
            e.printStackTrace();
        }
    }


    /**
     * add or edit exprot process
     *
     * @param loginUser
     * @param notDSExistsParam
     */
    private void addEditExportProcess(User loginUser, AddEditProcessParam notDSExistsParam, String exportDbName, String exportTableName) throws Exception {
        //转换入参
        AirflowDAGAddParam airflowDAGAddParam = processServiceImpl.convertAddEditProcessParam(notDSExistsParam, loginUser);

        //保存工作流
        Map<String, Object> addScheduleResult = dagConverterService.dagConverter(airflowDAGAddParam);

        if (Optional.ofNullable(addScheduleResult).map(e -> !Objects.equals(Status.SUCCESS, e.get(Constants.STATUS))).orElse(true)) {
            throw new ProcessConvertException(String.format("生成/修改工作流调度失败，详情：%s", JSON.toJSONString(addScheduleResult)));
        }

        Long processId = notDSExistsParam.getProcessId();

        //修改工作流时，先删除之前数据，再保存
        if (processId != null) {
            Map<String, Object> conditionParam = Maps.newHashMapWithExpectedSize(1);
            conditionParam.put("process_id", processId);
            List<OutPutEntity> outPutEntities = outPutMapper.selectByMap(conditionParam);
            if (CollectionUtils.isNotEmpty(outPutEntities)) {

                //删除依赖数据
                Map<String, Object> depConditionParam = Maps.newHashMapWithExpectedSize(1);
                depConditionParam.put("owning_process_id", processId);
                zrDepMapper.deleteByMap(depConditionParam);

                //删除输出数据
                OutPutEntity outPutEntity = outPutEntities.stream().findFirst().orElse(null);
                outPutMapper.deleteById(outPutEntity.getId());

                //删除导出数据
                List<OutPutExportEntity> exportEntities = outPutExportMapper.selectByMap(conditionParam);
                OutPutExportEntity outPutExportEntity = exportEntities.stream().findFirst().orElse(null);
                outPutExportMapper.deleteById(outPutExportEntity.getId());

                //删除执行计划数
                zrScheduleMapper.deleteByMap(conditionParam);

            }
        }

        Long returnProcessId = (Long) addScheduleResult.get(ProcessConstant.PROCESSDEFINITIONID);

        //保存依赖数据
        if (notDSExistsParam.getDepDOList() != null && notDSExistsParam.getDepDOList().size() > 0) {
            for (DepDO depDO : notDSExistsParam.getDepDOList()) {
                DepEntity depEntity = new DepEntity();
                BeanUtils.copyProperties(depDO, depEntity);
                depEntity.wrap(returnProcessId);
                zrDepMapper.insert(depEntity);
            }
        }

        //保存输出数据
        SqlParsingVO.OutPutDO outPutDO = notDSExistsParam.getOutPutDO();
        outPutMapper.insert(new OutPutEntity(returnProcessId, outPutDO.getDbName(), outPutDO.getTableName(), outPutDO.getTableType(), outPutDO.getValidDays()));

        //保存导出数据
        SqlParsingVO.ExportDO exportDO = notDSExistsParam.getExportDO();
        if (exportDO != null) {
            OutPutExportEntity outPutExportEntity = new OutPutExportEntity(returnProcessId, exportDO.getDatasourceName(), exportDO.getDatasourceJson());
            outPutExportMapper.insert(outPutExportEntity);
        }

        //保存执行计划数据
        zrScheduleMapper.insert(new ScheduleEntity(returnProcessId, notDSExistsParam.getCycle(), notDSExistsParam.getDateValue(), notDSExistsParam.getTimeValue()));

        taskService.execHivePartitionSensorAfterUpdate();

        //执行该工作流
        startExportProcess(loginUser, notDSExistsParam.getProjectName(), returnProcessId);
    }

    private AddEditProcessParam getNotExistsParam(User loginUser, Long processId, String dbName, String tableName, String dataSourceName, Integer partitionDays) {
        AddEditProcessParam addEditProcessParam = new AddEditProcessParam();
        String dbTableName = dbName + "." + tableName;
        //将导出表作为输出表
        SqlParsingVO.OutPutDO outPutDO = new SqlParsingVO.OutPutDO(dbName, tableName, "分区表");
        addEditProcessParam.setOutPutDO(outPutDO);

        //导出任务
        String datasourceJson = "{\"retentionDays\":" + partitionDays + "}";
        SqlParsingVO.ExportDO exportDO = new SqlParsingVO.ExportDO();
        exportDO.wrap(dataSourceName, datasourceJson);
        addEditProcessParam.setExportDO(exportDO);

        addEditProcessParam.setProcessId(processId);

        addEditProcessParam.setCycle("每日");
        addEditProcessParam.setTimeValue("05:00");
        addEditProcessParam.setDescription("数据导出：" + dbTableName);
        addEditProcessParam.setName(tableName);

        //获取projectid 和 projectname
        addEditProcessParam.setProjectId(getProjectId(loginUser));
        addEditProcessParam.setProjectName(getProjectName(loginUser));

        return addEditProcessParam;
    }

    /**
     * warp the parameter, change datasourcename and partitiondays, when export task exists
     *
     * @param loginUser
     * @param processDefinitionId
     * @param dataSourceName
     * @param partitionDays
     * @return
     */
    private AddEditProcessParam getExportTaskExistsParam(User loginUser, Long processDefinitionId, String dataSourceName, Integer partitionDays) {
        AddEditProcessParam addEditProcessParam = new AddEditProcessParam();
        ProcessDetailVO processDetailVO = processService.queryProcessDefinitionById(loginUser, processDefinitionId);
        addEditProcessParam.setCycle(processDetailVO.getCycle());
        addEditProcessParam.setDateValue(processDetailVO.getDateValue());
        List<DepDO> depDOList = new ArrayList<>();
        for (ProcessDetailVO.DepDO depDO : processDetailVO.getDepDOList()) {
            DepDO item = new DepDO(depDO.getProjectId(), depDO.getProcessId(), depDO.getProcessName(),
                    depDO.getTaskName(), depDO.getFullTaskName(), depDO.getTableName(), depDO.getOwnerName(), depDO.getCycle(), depDO.getDateValue());
            depDOList.add(item);
        }
        addEditProcessParam.setDepDOList(depDOList);
        addEditProcessParam.setDescription(processDetailVO.getDescription());
        String datasourceJson = "{\"retentionDays\":" + partitionDays + "}";
        SqlParsingVO.ExportDO exportDO = processDetailVO.getExportDO();
        exportDO.wrap(dataSourceName, datasourceJson);
        addEditProcessParam.setExportDO(exportDO);
        addEditProcessParam.setName(processDetailVO.getName());
        addEditProcessParam.setOutPutDO(processDetailVO.getOutPutDO());
        addEditProcessParam.setProcessId(processDetailVO.getProcessId());
        addEditProcessParam.setSql(processDetailVO.getSql());
        addEditProcessParam.setTimeValue(processDetailVO.getTimeValue());
        addEditProcessParam.setProjectId(processDetailVO.getProjectId());
        addEditProcessParam.setProjectName(processDetailVO.getProjectName());
        return addEditProcessParam;
    }

    /**
     * gets the projectName to which the user belongs
     *
     * @param loginUser
     * @return
     */
    private String getProjectName(User loginUser) {
        Map<String, Object> userGroupCondition = Maps.newHashMapWithExpectedSize(1);
        userGroupCondition.put("user_id", loginUser.getId());
        List<UserGroup> userGroups = userGroupMapper.selectByMap(userGroupCondition);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(userGroups)) {
            throw new ProcessParamsException("无法获取到当前用户所属项目，请联系产品或研发~");
        }
        UserGroup userGroup = userGroups.get(0);
        Project project = projectMapper.selectById(userGroup.getProjectId());
        return Optional.ofNullable(project).map(Project::getName).orElse(null);
    }

    /**
     * gets the projectCode to which the user belongs
     *
     * @param loginUser
     * @return
     */
    private int getProjectId(User loginUser) {
        Map<String, Object> userGroupCondition = Maps.newHashMapWithExpectedSize(1);
        userGroupCondition.put("user_id", loginUser.getId());
        List<UserGroup> userGroups = userGroupMapper.selectByMap(userGroupCondition);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(userGroups)) {
            throw new ProcessParamsException("无法获取到当前用户所属项目，请联系产品或研发~");
        }
        return userGroups.get(0).getProjectId();
    }


    /**
     * 检查 clickhouse 表是否存在
     * @param dbName
     * @param tableName
     */
    @Override
    public boolean isExistClickhouseTable(String dbName, String tableName) {
        // 检查是否存在工作流
        Map<String, Object> dataMap = exportDataService.getProcessId(dbName, tableName);
        Map<String, Object> processIdMap = (Map) dataMap.get(Constants.DATA_LIST);
        Long processDefinitionId = (Long) processIdMap.get("processDefinitionId");

        if (processDefinitionId == null && clickHouseUtil.isExistClickhouseTable(dbName, tableName)) {
            // 工作流不存在并且相应的 clickhouse 表存在
            return true;
        }
        return false;
    }
}
