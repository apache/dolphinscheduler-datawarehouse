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

import static com.cronutils.model.CronType.QUARTZ;
import static com.cronutils.model.field.expression.FieldExpressionFactory.always;
import static com.cronutils.model.field.expression.FieldExpressionFactory.on;
import static com.cronutils.model.field.expression.FieldExpressionFactory.questionMark;
import static com.datawarehouse.be.constants.ProcessConstant.PROCESSDEFINITIONID;

import com.alibaba.fastjson.JSON;
import com.cronutils.builder.CronBuilder;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.field.value.SpecialChar;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.datawarehouse.be.configuration.SensorConfig;
import com.datawarehouse.be.constants.ProcessConstant;
import com.datawarehouse.be.dao.InputTableDAO;
import com.datawarehouse.be.enums.ProcessCronEnum;
import com.datawarehouse.be.enums.TaskCronEnum;
import com.datawarehouse.be.exceptions.*;
import com.datawarehouse.be.pojo.dos.CustomWarningDO;
import com.datawarehouse.be.pojo.dos.DepDO;
import com.datawarehouse.be.pojo.dos.ProcessDefinitionIODO;
import com.datawarehouse.be.pojo.dto.*;
import com.datawarehouse.be.pojo.params.AddEditProcessParam;
import com.datawarehouse.be.pojo.params.AirflowDAGAddParam;
import com.datawarehouse.be.pojo.params.SqlParsingParam;
import com.datawarehouse.be.pojo.vo.ProcessDetailVO;
import com.datawarehouse.be.pojo.vo.SqlParsingVO;
import com.datawarehouse.be.service.*;
import lombok.SneakyThrows;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.ExecutorService;

import org.apache.dolphinscheduler.api.service.ProcessDefinitionService;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.FailureStrategy;
import org.apache.dolphinscheduler.common.enums.IOType;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.RunMode;
import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.common.model.DependentItem;
import org.apache.dolphinscheduler.common.model.DependentTaskModel;
import org.apache.dolphinscheduler.common.task.sql.SqlParameters;
import org.apache.dolphinscheduler.common.utils.HiveSqlParser;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.SqlParserUtil;
import org.apache.dolphinscheduler.dao.entity.*;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class ProcessServiceImpl implements ProcessService {

    private static final Logger logger = LoggerFactory.getLogger(ProcessServiceImpl.class);

    @Autowired
    private ProcessDefinitionIOService ziroomProcessDefinitionIOService;

    @Autowired
    private ProcessDefinitionMapper processDefinitionMapper;

    @Autowired
    private DagConverterService dagConverterService;

    @Autowired
    private CustomWarningService customWarningService;

    @Autowired
    private VisualProcessService visualProcessService;

    @Autowired
    private ZROutPutMapper outPutMapper;

    @Autowired
    private ZROutPutExportMapper outPutExportMapper;

    @Autowired
    private ZRDepMapper zrDepMapper;

    @Autowired
    private ZRScheduleMapper zrScheduleMapper;

    @Autowired
    private UserGroupMapper userGroupMapper;

    @Autowired
    private ProjectMapper projectMapper;

    @Autowired
    private ProcessDefinitionIOMapper processDefinitionIOMapper;

    @Autowired
    private SensorConfig sensorConfig;

    @Autowired
    private InputTableDAO inputTableDAO;

    @Autowired
    private ExecutorService execService;

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private ProcessDefinitionService processDefinitionService;

    @Autowired
    private ProcessDefinitionMapper processDefineMapper;

    @Autowired
    private org.apache.dolphinscheduler.service.process.ProcessService processService;

    @Autowired
    private DependentService dependentService;

    @Override
    public ProcessDetailVO queryProcessDefinitionById(User loginUser, Long processId) {

        if (processId == null) {
            return null;
        }

        ProcessDefinition processDefinition;

        try {
            processDefinition = processDefinitionMapper.queryByCode(processId);

            if (processDefinition == null) {
                throw new ProcessDetailException(String.format("未获取到当前工作流，请确认工作流ID:%s是否正确！", processId));
            }
        } catch (Exception e) {
            throw new ProcessDetailException(String.format("获取工作流详情异常，请联系产品和研发，详情：%s", e.toString()));
        }
        return wrapProcessDetailVO(processDefinition);
    }

    private Long getProjectCode(User loginUser) {
        Map<String, Object> userGroupCondition = Maps.newHashMapWithExpectedSize(1);
        userGroupCondition.put("user_id", loginUser.getId());
        List<UserGroup> userGroups = userGroupMapper.selectByMap(userGroupCondition);
        if (CollectionUtils.isEmpty(userGroups)) {
            throw new ProcessParamsException("无法获取到当前用户所属项目，请联系产品或研发~");
        }
        UserGroup userGroup = userGroups.get(0);
        Project project = projectMapper.selectById(userGroup.getProjectId());
        return Optional.ofNullable(project).map(Project::getCode).orElse(null);
    }

    public UserGroup getUserGroup(User loginUser) {
        Map<String, Object> userGroupCondition = Maps.newHashMapWithExpectedSize(1);
        userGroupCondition.put("user_id", loginUser.getId());
        List<UserGroup> userGroups = userGroupMapper.selectByMap(userGroupCondition);
        if (CollectionUtils.isEmpty(userGroups)) {
            throw new ProcessParamsException("无法获取到当前用户所属信息，请联系产品或研发~");
        }
        return userGroups.get(0);
    }

    @Transactional(rollbackFor = Exception.class)
    @Override
    public void addEditProcess(AddEditProcessParam addEditProcessParam, User loginUser) throws Exception {
        addEditProcessParam.verify();
        for (DepDO depDO : addEditProcessParam.getDepDOList()) {
            if (depDO.getProcessId() == sensorConfig.processId) {
                depDO.setProcessId(null);
            }
            if (Objects.equals(depDO.getProcessName(), "hive_partition_sensor")){
                depDO.setProcessName(depDO.getTaskName());
            }
            if (Objects.equals(depDO.getTaskName(), "ALL")){
                depDO.setTaskName(depDO.getProcessName());
            }
        }
        Long processId = addEditProcessParam.getProcessId();

        Map<String, Set<String>> ioMap = checkSql(addEditProcessParam.getSql(), loginUser, processId);

        // 校验SQl中输出表是否合法
        checkOutPut(ioMap.get(SqlParserUtil.OUT_PUT), processId);

        // 转换入参
        AirflowDAGAddParam airflowDAGAddParam = convertAddEditProcessParam(addEditProcessParam, loginUser);

        // 保存/修改工作流
        Map<String, Object> createUpdateScheduleResult = dagConverterService.dagConverter(airflowDAGAddParam);

        if (Optional.ofNullable(createUpdateScheduleResult).map(e -> !Objects.equals(Status.SUCCESS, e.get(Constants.STATUS))).orElse(true)) {
            throw new ProcessConvertException(String.format("生成/修改工作流调度失败，详情：%s", JSON.toJSONString(createUpdateScheduleResult)));
        }

        if (processId != null) {
            Map<String, Object> conditionParam = Maps.newHashMapWithExpectedSize(1);
            conditionParam.put("process_id", processId);
            List<OutPutEntity> outPutEntities = outPutMapper.selectByMap(conditionParam);
            if (CollectionUtils.isNotEmpty(outPutEntities)) {

                // 删除依赖数据
                Map<String, Object> depConditionParam = Maps.newHashMapWithExpectedSize(1);
                depConditionParam.put("owning_process_id", processId);
                zrDepMapper.deleteByMap(depConditionParam);

                // 删除输出数据
                OutPutEntity outPutEntity = outPutEntities.stream().findFirst().orElse(null);
                outPutMapper.deleteById(outPutEntity.getId());

                // 删除导出数据
                List<OutPutExportEntity> exportEntities = outPutExportMapper.selectByMap(conditionParam);
                if (CollectionUtils.isNotEmpty(exportEntities)) {
                    OutPutExportEntity exportEntity = exportEntities.stream().findFirst().orElse(null);
                    outPutExportMapper.deleteById(exportEntity.getId());
                }

                // 删除告警信息
                customWarningService.deleteCustomWarning(processId);

                // 删除可视化非可视化请求参数
                visualProcessService.deleteVisualQueryDto(processId);

                // 删除执行计划数
                zrScheduleMapper.deleteByMap(conditionParam);
            }
        }
        Long returnProcessId = (Long) createUpdateScheduleResult.get(PROCESSDEFINITIONID);
        // 保存输出数据
        SqlParsingVO.OutPutDO outPutDO = addEditProcessParam.getOutPutDO();
        outPutMapper.insert(new OutPutEntity(returnProcessId, outPutDO.getDbName(), outPutDO.getTableName(), outPutDO.getTableType(), outPutDO.getValidDays()));

        // 保存导出数据
        SqlParsingVO.ExportDO exportDO = addEditProcessParam.getExportDO();
        if (exportDO != null) {
            OutPutExportEntity outPutExportEntity = new OutPutExportEntity((long) returnProcessId, exportDO.getDatasourceName(), exportDO.getDatasourceJson());
            outPutExportMapper.insert(outPutExportEntity);
        }

        // 保存执行计划数据
        zrScheduleMapper.insert(new ScheduleEntity(returnProcessId, addEditProcessParam.getCycle(), addEditProcessParam.getDateValue(), addEditProcessParam.getTimeValue()));

        // 添加报警
        customWarningService.addCustomWarning(returnProcessId, addEditProcessParam.getCustomWarningDO());

        // 添加可视化非可视化请求参数
        visualProcessService.addVisualQueryDto(addEditProcessParam.getQueryDto(), returnProcessId);

        //如果新增修改工作流导致 common 探分区工作流有更新，则启动执行整个common 工作流
            for (DepDO depDO : addEditProcessParam.getDepDOList()) {
                if(depDO.getProjectId() == sensorConfig.projectId){
                    ProcessDefinition processDefinition = processDefinitionMapper.queryByDefineName(depDO.getProjectId(), depDO.getProcessName());
                    // 初始化入参
                    FailureStrategy failureStrategy = FailureStrategy.CONTINUE;
                    WarningType warningType = WarningType.NONE;
                    TaskDependType taskDependType = TaskDependType.TASK_POST;
                    RunMode runMode = RunMode.RUN_MODE_SERIAL;
                    Priority processInstancePriority = Priority.MEDIUM;
                    int warningGroupId = 0;
                    String workerGroup = "default";
                    Integer timeout = Constants.MAX_TASK_TIMEOUT;
                    CommandType execType = CommandType.START_PROCESS;
                    String scheduleTime = null;
                    Long processDefinitionCode = processDefinition.getCode();
                    String projectOwnerName = sensorConfig.projectOwner;
                    User user = userMapper.queryByUserNameAccurately(projectOwnerName);
                    Map<String, Object> execProcessInstance = execService.execProcessInstance(
                        user,
                        sensorConfig.projectId,
                        processDefinitionCode,
                        scheduleTime,
                        execType,
                        failureStrategy,
                        null,
                        taskDependType,
                        warningType,
                        warningGroupId,
                        runMode,
                        processInstancePriority,
                        workerGroup,
                        null,
                        timeout,
                        null,
                        null,
                        0
                    );
                    if (Optional.ofNullable(execProcessInstance).map(e -> !Objects.equals(Status.SUCCESS, e.get(Constants.STATUS))).orElse(true)) {
                        throw new ProcessConvertException(String.format("保存sql结束后执行探分区任务失败，详情：%s", JSON.toJSONString(execProcessInstance)));
                    }
                }
            }
    }

    private ProcessDetailVO wrapProcessDetailVO(ProcessDefinition process) {
        Long processCode = process.getCode();
        String processName = process.getName();
        String desc = process.getDescription();
        long ProjectCode = process.getProjectCode();
        Project project = projectMapper.selectById(ProjectCode);
        String projectName = Optional.ofNullable(project).map(e -> e.getName()).orElse(null);
        ProcessDefinition processDefinition = processDefineMapper.queryByCode(processCode);
        List<ProcessTaskRelation> processTaskRelations = processService.findRelationByCode(processDefinition.getCode(), processDefinition.getVersion());
        List<TaskDefinitionLog> taskDefinitionLogs = processService.getTaskDefineLogListByRelation(processTaskRelations);
        Optional<TaskDefinitionLog> res = taskDefinitionLogs.stream().filter(x -> x.getTaskType().equals("SQL")).findFirst();
        String sql = "";
        if (res.isPresent()){
            SqlParameters sqlParameters = JSONUtils.parseObject(res.get().getTaskParams(), SqlParameters.class);
            sql = sqlParameters.getSql();
        }
        // 获取工作流执行计划
        Map<String, Object> conditionMap = Maps.newHashMapWithExpectedSize(1);
        conditionMap.put("process_id", processCode);
        List<ScheduleEntity> schedules = zrScheduleMapper.selectByMap(conditionMap);
        String cycle = null;
        String dateValue = null;
        String timeValue = null;
        if (CollectionUtils.isNotEmpty(schedules)) {
            ScheduleEntity scheduleEntity = schedules.get(0);
            cycle = Optional.ofNullable(scheduleEntity).map(e -> e.getCycle()).orElse(null);
            dateValue = Optional.ofNullable(scheduleEntity).map(e -> e.getDateValue()).orElse(null);
            timeValue = Optional.ofNullable(scheduleEntity).map(e -> e.getTimeValue()).orElse(null);
        }

        // 获取工作流依赖任务
        Map<String, Object> depConditionMap = Maps.newHashMapWithExpectedSize(1);
        depConditionMap.put("owning_process_id", processCode);
        List<DepEntity> depEntityList = zrDepMapper.selectByMap(depConditionMap);
        List<ProcessDetailVO.DepDO> depDOList = null;
        if (CollectionUtils.isNotEmpty(depEntityList)) {
            depDOList = Lists.newArrayList();
            for (DepEntity item : depEntityList) {
                ProcessDetailVO.DepDO depDO = new ProcessDetailVO.DepDO();
                BeanUtils.copyProperties(item, depDO);
                depDOList.add(depDO);
            }
        }

        // 获取工作流输出
        List<OutPutEntity> outPutEntityList = outPutMapper.selectByMap(conditionMap);
        SqlParsingVO.OutPutDO outPutDO = null;
        if (CollectionUtils.isNotEmpty(outPutEntityList)) {
            OutPutEntity outPutEntity = outPutEntityList.get(0);
            outPutDO = new SqlParsingVO.OutPutDO();
            BeanUtils.copyProperties(outPutEntity, outPutDO);
        }

        // 获取工作流导出
        SqlParsingVO.ExportDO exportDO = null;
        List<OutPutExportEntity> exportEntities = outPutExportMapper.selectByMap(conditionMap);
        if (CollectionUtils.isNotEmpty(exportEntities)) {
            OutPutExportEntity outPutExportEntity = exportEntities.get(0);
            exportDO = new SqlParsingVO.ExportDO();
            exportDO.wrap(outPutExportEntity.getDatasourceName(), outPutExportEntity.getDatasourceJson());
        }

        // 封装可视化请求参数
        QueryDto queryDto = visualProcessService.findQueryDtoById(processCode);

        // 获取工作流的告警信息
        CustomWarningDO customWarningDO = customWarningService.getCustomWarningParam(processCode);

        // 封装
        return new ProcessDetailVO(customWarningDO,queryDto, processCode, sql, processName, desc, cycle, dateValue, timeValue, depDOList, outPutDO, exportDO, ProjectCode, projectName);
    }

    /**
     * 转换AddEditProcessParam
     */
    public AirflowDAGAddParam convertAddEditProcessParam(AddEditProcessParam param, User loginUser) {
        // 获取用户信息，以及对应的项目、租户信息
        Long processId = param.getProcessId();
        // 拿到项目名称，例如：运营管理中心
        Long projectCode = processId == null ? getProjectCode(loginUser) : param.getProjectId();
        UserGroup userGroup = getUserGroup(loginUser);
        Integer tenantId = Optional.ofNullable(userGroup).map(UserGroup::getTenantId).orElseThrow(() -> new ProcessParamsException("未获取到当前用户租户id，请联系产品和研发~"));
        String userName = loginUser.getUserName();
        List<String> emails = Lists.newArrayList(loginUser.getEmail());

        // 初始化AddEditProcessParam默认值
        param.init();
        List<TasksDTO> tasks = new ArrayList<>();
        // 正常的调度任务 【有前置的依赖节点 -> sql -> python】
        if (param.getSql() != null) {
            // 封装入参AirflowDAGAddParam中Task对象
            TasksDTO<SQLTypeParams> sqlTask = new TasksDTO<>();
            sqlTask.init();
            for (DepDO e : param.getDepDOList()) {
                TasksDTO<DependenceTypeParams> depTask = new TasksDTO<>();
                depTask.init();
                List<DependentItem> dependItemList = new ArrayList<>();
                List<DependentTaskModel> dependentTaskModels = new ArrayList<>();
                String cycle = e.getCycle();
                String dateValueParam = e.getDateValue();
                String cycleEN = Optional.ofNullable(TaskCronEnum.getCycleEN(cycle)).orElseThrow(() -> new ProcessParamsException(String.format("依赖的入参周期(cycle:%s)未定义，请修改后重试~", cycle)));
                String dateValue = Optional.ofNullable(TaskCronEnum.getDateValue(cycle, dateValueParam)).orElseThrow(() -> new ProcessParamsException(String.format("依赖的入参周期(cycle:%s)和日期(dateValue:%s)未定义，请修改后重试~", cycleEN, dateValueParam)));

                if (e.getProjectId() == sensorConfig.projectId && !TaskCronEnum.DAY.getCycleEN().equals(cycleEN)) {
                    throw new ServiceException(String.format("默认探分区任务 %s 依赖周期最小修改粒度为天", e.getFullTaskName()));
                }
                if (Optional.ofNullable(e.getProcessId()).isPresent()){
                    dependItemList.add(new DependentItem(e.getProjectId(), e.getProcessId(), 0, cycleEN, dateValue));
                }else{
                    dependItemList.add(new DependentItem(e.getProjectId(), 0, cycleEN, dateValue));
                }
                DependentTaskModel dtm = new DependentTaskModel();
                dtm.setDependItemList(dependItemList);
                dependentTaskModels.add(dtm);
                DependenceTypeParams dependenceTypeParams = new DependenceTypeParams();
                DependenceDTOV2 dependence = new DependenceDTOV2();
                dependence.setDependTaskList(dependentTaskModels);
                dependenceTypeParams.setDependence(dependence);
                depTask.setTaskParams(dependenceTypeParams);
                depTask.setName(e.getProcessName());
                depTask.setTaskType("DEPENDENT");
                tasks.add(depTask);
            }

            // 封装SQL任务
            sqlTask.wrap(ProcessConstant.SQL_TASK_NAME,"SQL");
            SQLTypeParams sqlParamsParams = new SQLTypeParams();
            sqlParamsParams.setSql(param.getSql());
            Integer datasourceId = Optional.ofNullable(userGroup).map(UserGroup::getInitDatasourceIds).map(e -> Integer.valueOf(e)).orElseThrow(() -> new ProcessParamsException("未获取到当前用户数据源id，请联系产品和研发~"));
            sqlParamsParams.wrap(datasourceId);
            sqlTask.setTaskParams(sqlParamsParams);

            // 封装入参AirflowDAGAddParam
            tasks.add(sqlTask);

        }

        // 数据导出任务【仅有一个python节点】
        SqlParsingVO.OutPutDO outPutDO = param.getOutPutDO();
        TasksDTO<PythonTypeParams> exportTask = null;
        if (outPutDO != null && param.getExportDO() != null && StringUtils.isNotBlank(param.getExportDO().getDatasourceName())) {
            exportTask = new TasksDTO();
            exportTask.wrap(ProcessConstant.EXPORT_TASK_NAME, "PYTHON");
            exportTask.init();

            PythonTypeParams pythonParamsDTO = new PythonTypeParams();

            String dbName = outPutDO.getDbName();
            String tableName = outPutDO.getTableName();
            SqlParsingVO.ExportDO exportDO = param.getExportDO();
            SqlParsingVO.ExportDO.ClickhouseDO clickhouseDO = JSON.parseObject(exportDO.getDatasourceJson(), SqlParsingVO.ExportDO.ClickhouseDO.class);
            int retentionDays = clickhouseDO.getRetentionDays();
            pythonParamsDTO.setRawScript(String.format("from clickhouse.youdata_clickhouse_operator import YouDataClickHouseOperator \nycho=YouDataClickHouseOperator(hive_db='%s', hive_table='%s', partition_nums=%s) \nycho.execute()", dbName, tableName, retentionDays));

            LocalParamsDTO localParamsDTO = new LocalParamsDTO();
            localParamsDTO.setProp("type");
            localParamsDTO.setDirect("IN");
            localParamsDTO.setType("VARCHAR");
            localParamsDTO.setValue("clickhouse");

            pythonParamsDTO.setLocalParams(Lists.newArrayList(localParamsDTO));

            ResourceListDTO resourceListDTO1 = new ResourceListDTO(4);
            ResourceListDTO resourceListDTO2 = new ResourceListDTO(11);
            ResourceListDTO resourceListDTO3 = new ResourceListDTO(6);
            ResourceListDTO resourceListDTO4 = new ResourceListDTO(10);
            ResourceListDTO resourceListDTO5 = new ResourceListDTO(9);
            ResourceListDTO resourceListDTO6 = new ResourceListDTO(8);
            ResourceListDTO resourceListDTO7 = new ResourceListDTO(7);
            ResourceListDTO resourceListDTO8 = new ResourceListDTO(5);
            pythonParamsDTO.setResourceList(Lists.newArrayList(resourceListDTO1, resourceListDTO2, resourceListDTO3, resourceListDTO4, resourceListDTO5, resourceListDTO6, resourceListDTO7, resourceListDTO8));

            exportTask.setTaskParams(pythonParamsDTO);
        }

        if (exportTask != null) {
            tasks.add(exportTask);
        }

        AirflowDAGAddParam airflowDAGAddParam = new AirflowDAGAddParam(param.getProcessId()
            , projectCode
            , param.getName()
            , param.getDescription()
            , convertCron(param.getCycle(), param.getDateValue(), param.getTimeValue())
            , userName
            , emails
            , param.getTaskCount()
            , param.getStartDate()
            , param.getRetries()
            , param.getPool()
            , new ProcessJsonDODTO(tasks, tenantId));

        logger.info("调用convertAddEditProcessParam方法,入参:\nAddEditProcessParam:\n{}\n,loginUser:\n{}\n,出参:\nAirflowDAGAddParam:\n{}", JSON.toJSONString(airflowDAGAddParam), JSON.toJSONString(loginUser), JSON.toJSONString(airflowDAGAddParam));
        return airflowDAGAddParam;
    }

    // 转换Cron表达式
    public String convertCron(String cycle, String dateValue, String timeValue) {

        // 校验小时，分钟
        String[] timeArray = timeValue.split(":");
        String hour = timeArray[0];
        if (StringUtils.isBlank(hour)) {
            throw new ProcessCycleFormatException(String.format("任务定时中入参小时(%s)格式错误，必须为2位小数，请确认~", hour));
        } else {
            if (hour.length() != 2) {
                throw new ProcessCycleFormatException(String.format("任务定时中入参小时(%s)格式错误，必须为2位小数，请确认~", hour));
            }
        }
        String min = timeArray[1];
        if (StringUtils.isBlank(min)) {
            throw new ProcessCycleFormatException(String.format("任务定时中入参分钟(%s)不能为空，请确认~", min));
        } else {
            if (min.length() != 2) {
                throw new ProcessCycleFormatException(String.format("任务定时中入参分钟(%s)格式错误，必须为2位数字，请确认~", min));
            }
        }

        // 初始化默认cron表达式
        CronBuilder cronBuilder = CronBuilder.cron(CronDefinitionBuilder.instanceDefinitionFor(QUARTZ));
        cronBuilder
                // 每年
                .withYear(always())
                // 每月
                .withMonth(always())
                // 设置小时
                .withHour(on(Integer.valueOf(hour)))
                // 设置分钟
                .withMinute(on(Integer.valueOf(min)))
                // 0秒开始
                .withSecond(on(0))
        ;

        if (ProcessCronEnum.get(cycle) == ProcessCronEnum.TODAY) {
            // 每天执行
            cronBuilder
                    .withDoW(questionMark())
                    .withDoM(always())
            ;
        } else if (ProcessCronEnum.get(cycle) == ProcessCronEnum.WEEKLY) {
            // 指定周几执行
            cronBuilder
                    .withDoW(on(ProcessCronEnum.getDayOfWeek(dateValue)))
                    .withDoM(questionMark())
            ;
        } else if (ProcessCronEnum.get(cycle) == ProcessCronEnum.MONTHLY) {
            // 指定每月几日执行
            cronBuilder
                    .withDoW(questionMark());
            String dateValueExp = ProcessCronEnum.getDayOfMonth(dateValue);
            if ("L".equals(dateValueExp)) {
                // 最后一天
                cronBuilder.withDoM(on(SpecialChar.valueOf(dateValueExp)));
            } else {
                cronBuilder.withDoM(on(Integer.valueOf(dateValueExp)));
            }
        } else {
            throw new ProcessParamsException("暂不支持的周期和日期入参，请确认~");
        }
        return cronBuilder.instance().asString();
    }

    @Override
    public SqlParsingVO sqlParsing(SqlParsingParam sqlParsingParam, User loginUser) {
        // 校验
        sqlParsingParam.verify();

        // 获取入参
        String sql = sqlParsingParam.getSql();

        Long processId = sqlParsingParam.getProcessId();

        // 校验sql，并获取SQL中输入表、输出表
        Map<String, Set<String>> ioMap = checkSql(sql, loginUser, processId);

        // 校验输出表中是否存在非动态分区
        String warningMsg = SqlParserUtil.whetherFixedPartitionsInOutputTable(sql);

        // 获取输入表对应依赖任务，默认分区为昨天
        List<SqlParsingVO.DepDO> depDOList = getDepTaskList(ioMap.get(SqlParserUtil.INPUT), "$[yyyyMMdd-1]");

        // 获取输出表信息
        List<SqlParsingVO.OutPutDO> outPutDOList = getExportTable(ioMap.get(SqlParserUtil.OUT_PUT));

        // 返回结果
        return new SqlParsingVO(depDOList, outPutDOList, warningMsg);
    }

    // 校验sql
    private Map<String, Set<String>> checkSql(String sql, User loginUser, Long processId) {
        // 获取SQL中输入表、输出表
        Map<String, Set<String>> ioMap = getIO(sql);
        if (MapUtils.isEmpty(ioMap)) {
            throw new SQLIOException(String.format("SQL中没有任务查询表和写入表，请修改为只有一张写入表以及至少一个查询表~", sql));
        }

        // 检查输入输出格式
        checkIOFormat(ioMap);

        // 校验SQl中输出表是否合法
        checkOutPut(ioMap.get(SqlParserUtil.OUT_PUT), processId);

        // 检查输入输出表是否有循环依赖的情况
        checkCircularDependencies(ioMap);

        // 校验输入、输出流是否合法
        checkIO(sql, loginUser.getUserName());

        return ioMap;
    }

    /**
     * 获取输出表信息
     */
    private List<SqlParsingVO.OutPutDO> getExportTable(Set<String> set) {
        return set.stream().map(e -> e.split("\\.")).map(e -> new SqlParsingVO.OutPutDO(e[0], e[1], "分区表")).collect(Collectors.toList());
    }

    @SneakyThrows
    private void checkoutTablePermission(String sql, String userName, List<String> setVariableList) {
//        boolean result = inputTableDAO.isHavingPermissionOfTable(userName, sql, setVariableList);
//
//        System.out.println("aaa");
//        List<String> sqlArray = SqlParserUtil.splitSql(sql);
//        for (String item : sqlArray) {
//            boolean result;
//            if(isUserDefineUdfSql(item)) {
//                try {
//                    result = inputTableDAO.isHavingPermissionOfTable(userName, new StringBuffer("explain ").append(item).toString(), setVariableList);
//                } catch (Exception e) {
//                    throw new SQLIOException(String.format("%s", e.toString()));
//                }
//                if (!result) {
//                    throw new SQLIOException(String.format("当前用户(%s)无查询和写入权限，请联系产品和研发开通，sql详情：%s", item));
//                }
//            }else {
//
//            }
//        }
    }

    /**
     * 检查输入输出
     */
    private void checkIO(String sql, String userName) {
        // 获取sql语法树
        List<HiveSqlParser> hiveSqlParsers = SqlParserUtil.getHiveSqlParser(sql);

        // 检验是否存在新建表,若不存在，校验整个SQL有无输入输出表权限
        boolean whetherExistsCreatingTable = hiveSqlParsers.stream().anyMatch(e -> e.getTableList().stream().anyMatch(i -> i.getOperationType().equals(HiveSqlParser.OperationType.CREATE)));

        // 提取sql中设置环境变量的sql
        List<String> envVariableList = SqlParserUtil.extractEnvOfSql(sql);

        if (!whetherExistsCreatingTable) {
            // 不存在新建表
            checkoutTablePermission(sql, userName, envVariableList);
        } else {
            HiveSqlParser.Table existsCreateTable = null;
            // 存在新建表
            for (HiveSqlParser item : hiveSqlParsers) {
                // 获取sql中输入输出表
                List<HiveSqlParser.Table> tables = item.getTableList();
                if (CollectionUtils.isEmpty(tables)) {
                    continue;
                }
                for (HiveSqlParser.Table table : tables) {
                    HiveSqlParser.OperationType operationType = table.getOperationType();
                    String tableSql = table.getSql();
                    if (operationType.equals(HiveSqlParser.OperationType.SELECT)) {
                        // 不在此校验查询表权限，后面统一校验
                        continue;
                    } else if (HiveSqlParser.OperationType.UPDATE.equals(operationType) || HiveSqlParser.OperationType.DELETE.equals(operationType)) {
                        // update、delete中sql不支持，因为sql中若依赖创建表，无法校验sql中的所表权限
                        throw new SQLIOException(String.format("暂不支持Update、Delete操作，请修改，sql详情：%s", tableSql));
                    } else if (HiveSqlParser.OperationType.CREATE.equals(operationType)) {
                        // 校验create表权限
                        checkoutTablePermission(tableSql, userName, envVariableList);
                        // 保存创建表
                        existsCreateTable = table;
                    } else if (HiveSqlParser.OperationType.DROP.equals(operationType)) {
                        // 校验drop表权限
                        checkoutTablePermission(tableSql, userName, envVariableList);
                    } else if (HiveSqlParser.OperationType.INSERT.equals(operationType)) {
                        // 校验创建表必须为输出表
                        String insertTableName = wrapTableName(table);
                        String existsCreateTableName = wrapTableName(existsCreateTable);

                        // 存在创建表，写入表和创建表须一致
                        if (StringUtils.isNotBlank(existsCreateTableName)) {
                            if (!StringUtils.equals(existsCreateTableName, insertTableName)) {
                                throw new SQLIOException(String.format("sql中写入表(%s)和创建表(%s)须一致，请修改~", insertTableName, existsCreateTableName));
                            }
                        } else {
                            // 校验create表权限
                            checkoutTablePermission(tableSql, userName, envVariableList);
                        }
                    }
                }
            }

            // 统一校验查询表权限，减少请求hive校验权限次数
            List<String> inputTableList = getInputTables(hiveSqlParsers);
            if (CollectionUtils.isNotEmpty(inputTableList)) {
                StringBuffer wrapSql = new StringBuffer();
                wrapSql.append("explain ");
                for (int i = 0, j = inputTableList.size(); i < j; i++) {
                    String item = inputTableList.get(i);
                    wrapSql.append("(select 1 from ");
                    wrapSql.append(item);
                    wrapSql.append(" limit 1 )");
                    if (i + 1 < j) {
                        wrapSql.append(" union all ");
                    }
                }
                // 设置非严格模式，只为了不影响校验表的查询权限
                StringBuffer wrapEnvSql = new StringBuffer("set hive.mapred.mode=false");
                boolean result;
                try {
                    result = inputTableDAO.isHavingPermissionOfTable(userName, wrapSql.toString(), wrapEnvSql.toString());
                } catch (SQLException e) {
                    throw new SQLIOException(String.format("%s", e));
                }
                if (!result) {
                    // 无权限
                    throw new SQLIOException(String.format("当前用户(%s)无查询表的权限，请联系产品和研发开通，sql详情：%s", userName, wrapEnvSql.append("\n").append(wrapSql)));
                }
            }
        }
    }

    private List<String> getInputTables(List<HiveSqlParser> hiveSqlParsers) {
        // 校验
        if (CollectionUtils.isEmpty(hiveSqlParsers)) {
            return null;
        }
        // 获取sql中临时表
        Set<String> inputTables = new HashSet();
        List<String> tempTableList = hiveSqlParsers.stream()
                .map(e -> e.getTempTableList())
                .filter(e -> CollectionUtils.isNotEmpty(e))
                .flatMap(e -> e.stream().map(i -> wrapTableName(i)))
                .filter(e -> StringUtils.isNotBlank(e))
                .collect(Collectors.toList());

        // 排除输入表中的临时表
        hiveSqlParsers.forEach(e -> {
                    for (HiveSqlParser.Table i : e.getTableList()) {
                        if (i.getOperationType().equals(HiveSqlParser.OperationType.SELECT)) {
                            String tableName = wrapTableName(i);
                            if (CollectionUtils.isNotEmpty(tempTableList) && !tempTableList.contains(tableName)) {
                                // 不验证临时表权限，故排除
                                inputTables.add(wrapTableName(i));
                            }
                        }
                    }
                }
        );
        return Lists.newArrayList(inputTables);
    }

    private String wrapTableName(HiveSqlParser.Table table) {
        if (table == null) {
            return null;
        }
        StringBuffer tableName = new StringBuffer();
        if (StringUtils.isNotBlank(table.getDbName())) {
            tableName.append(table.getDbName()).append(".").append(table.getTableName());
        } else {
            tableName.append(table.getTableName());
        }
        return tableName.toString();
    }

    /**
     * 检查输出流
     */
    private void checkOutPut(Set<String> outputSet, Long processId) {
        // 检查SQL中是否有且只有一个输出
        if (CollectionUtils.isEmpty(outputSet)) {
            throw new SQLIOException("SQL中没有任何写入表，请修改为只有一张写入表~");
        } else if (outputSet.size() > 1) {
            throw new SQLIOException(String.format("SQL中出现多张写入表%s，请修改为只有一张写入表~", JSON.toJSONString(outputSet)));
        }

        // 检查是否存在多个相同的输出（多个相同输出，写入同一张表，会出现数据一致性问题）
        List<String> outputList = Lists.newArrayList(outputSet);
        String tableName = outputList.get(0);
        List<ProcessDefinitionIO> processDefinitionIOS = processDefinitionIOMapper.queryByIONameAndIoType(tableName, IOType.OUTPUT.getCode());
        if (CollectionUtils.isNotEmpty(processDefinitionIOS)) {
            ProcessDefinitionIO processDefinitionIO = processDefinitionIOS.get(0);
            if (processId != null) {
                // 修改场景，如果修改后的输出表和之前一样，允许通过，如果跟其他工作流输出表一样，则出现冲突，禁止通过
                if (processDefinitionIO.getProcessDefinitionCode() != processId.intValue()) {
                    long processIOId = processDefinitionIO.getProcessDefinitionCode();
                    ProcessDefinition processDefinition = processDefinitionMapper.queryByDefineId(11L);
                    String processName = Optional.ofNullable(processDefinition).map(e -> e.getName()).orElse(null);
                    // 跟其他工作流输出表一样，则出现冲突，禁止通过
                    throw new SQLIOException(String.format("SQL中的写入表[%s]已存在于工作流[名称：%s，ID：%s]中，请写入到其他表~", tableName, processName, processIOId));
                }
            } else {
                // 新增场景
                long processIOId = processDefinitionIO.getProcessDefinitionCode();
                ProcessDefinition processDefinition = processDefinitionMapper.queryByDefineId(processIOId);
                String processName = Optional.ofNullable(processDefinition).map(e -> e.getName()).orElse(null);
                throw new SQLIOException(String.format("SQL中的写入表[%s]已存在于工作流[名称：%s，ID：%s]中，请写入到其他表~", tableName, processName, processIOId));
            }
        }
    }

    // 检查输入输出格式
    private void checkIOFormat(Map<String, Set<String>> ioMap) {
        ioMap.forEach((k, v) -> {
            v.forEach(e -> {
                if (StringUtils.isBlank(e)) {
                    throw new SqlIOFormatException("SQL中表名不能为空，请修改后重试~");
                }
            });
        });
    }

    /**
     * 获取依赖任务集合
     */
    public List<SqlParsingVO.DepDO> getDepTaskList(Set<String> set, String tablePartition) {
        // 校验
        if (CollectionUtils.isEmpty(set)) {
            return null;
        }

        // 结果
        List<SqlParsingVO.DepDO> result = Lists.newArrayList();

        // 获取用户定义的工作流血缘
        List<ProcessDefinitionIODO> processDefinitionIODOList = ziroomProcessDefinitionIOService.query(IOType.OUTPUT, Lists.newArrayList(set));
        if (CollectionUtils.isNotEmpty(processDefinitionIODOList)) {

            // 过滤掉由于sql查询条件不能区分输入表名称大小写导致导致匹配到多个输入表的依赖
            processDefinitionIODOList = processDefinitionIODOList.stream().filter(e -> e != null).filter(e -> set.stream().anyMatch(s -> StringUtils.equals(s, e.getIoName()))).collect(Collectors.toList());

            // 封装返回结果
            for (ProcessDefinitionIODO e : processDefinitionIODOList) {
                ProcessDefinition processDefinition = processDefinitionMapper.queryByDefineId(e.getProcessDefinitionId());
                // 依赖用户定义工作流，默认依赖所有任务
                String allTaskName = "ALL";
                SqlParsingVO.DepDO depDO = new SqlParsingVO.DepDO(processDefinition.getProjectCode(), e.getProcessDefinitionId(), processDefinition.getName(), e.getTaskId(), allTaskName, e.getIoName(), Optional.ofNullable(processDefinition).map(ProcessDefinition::getUserName).orElse(null), 0);
                result.add(depDO);
            }
        }

        // 删除已经获取依赖任务对应用户自定义的血缘
        if (CollectionUtils.isNotEmpty(processDefinitionIODOList)) {
            logger.info("删除前set:{}", JSON.toJSONString(set));
            logger.info("processDefinitionIODOList:{}", JSON.toJSONString(processDefinitionIODOList));
            List<ProcessDefinitionIODO> finalProcessDefinitionIODOList = processDefinitionIODOList;
            Set<String> sameTableSet = set.stream().filter(e -> finalProcessDefinitionIODOList.stream().anyMatch(s -> StringUtils.equalsIgnoreCase(s.getIoName(), e))).collect(Collectors.toSet());
            logger.info("sameTableSet:{}", JSON.toJSONString(sameTableSet));
            set.removeAll(sameTableSet);
            logger.info("删除后set:{}", JSON.toJSONString(set));
        }

        // 若未找到用户定义的工作流血缘，则查找默认工作流中依赖任务
        List<ProcessDefinition> defaultDefinitionList = dependentService.getDependentTasks(set, tablePartition);
        set.forEach(tableName -> {
            // 根据SQL中输入表，查找common项目中工作流中依赖任务
            String depName = wrapDepName(tableName, tablePartition);
            TasksDTO newDepTask = null;
            ProcessDefinition defaultProcessDefinition = null;
            if (CollectionUtils.isNotEmpty(defaultDefinitionList)) {
                // 依赖任务名称和工作流名称一致，故用工作流名称可以匹配依赖任务，来获取依赖工作流
                defaultProcessDefinition = defaultDefinitionList.stream().filter(e -> StringUtils.equalsIgnoreCase(e.getName(), depName)).findFirst().orElse(null);
                if (defaultProcessDefinition != null) {
                    newDepTask = dagConverterService.getDependentTask(defaultProcessDefinition, depName);
                }
            }

            // 若未找默认工作流中依赖任务，则生成一个返回
            if (newDepTask == null) {
                logger.info("未找到探分区工作流中的依赖任务：{}，生成新的依赖任务：{}", depName, depName);
                newDepTask = new TasksDTO();
                newDepTask.wrap(depName);
            }

            // 封装返回结果
            SqlParsingVO.DepDO depDO = new SqlParsingVO.DepDO(sensorConfig.projectId, sensorConfig.processId, sensorConfig.processName, String.valueOf(newDepTask.getCode()), newDepTask.getName(), tableName, Optional.ofNullable(defaultProcessDefinition).map(ProcessDefinition::getUserName).orElse(null), 1);
            result.add(depDO);
        });
        return result;
    }

    private String wrapDepName(String tableName, String tablePartition) {
        // depTasks的格式必须为:库名.表名.分区名.sensor
        return new StringBuffer(tableName).append(".").append(tablePartition).append(".sensor").toString();
    }

    private Map<String, Set<String>> getIO(String sql) {
        try {
            return SqlParserUtil.getIOOfSql(sql, true);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLIOException(String.format("获取SQL的依赖和输出异常, 详情：%s", e));
        }
    }

    /**
     * 检查输入输出表中是否有循环依赖
     */
    private void checkCircularDependencies(Map<String, Set<String>> ioMap) {

        Set<String> inputIoName = ioMap.get(SqlParserUtil.INPUT);

        // 查询所有的下游血缘
        Set<Long> ids = new HashSet<>();
        Set<String> ioNames = new HashSet<>(ioMap.get(SqlParserUtil.OUT_PUT));

        while (!ioNames.isEmpty()) {
            // 是否有以io_names 为输入表的工作流
            List<ProcessDefinitionIO> processDefinitionIOS1 =  processDefinitionIOMapper.queryByIoNamesAndIoType(ioNames, IOType.INPUT.getCode());
            if (!processDefinitionIOS1.isEmpty()) {
                for (ProcessDefinitionIO processDefinitionIO : processDefinitionIOS1) {
                    ids.add(processDefinitionIO.getProcessDefinitionCode());
                }
            } else {
                break;
            }

            // 清空上次循环查询结果
            ioNames.clear();

            // 查询获取 io_names
            List<ProcessDefinitionIO> processDefinitionIOS =  processDefinitionIOMapper.queryByProcessIdsAndIoType(ids, IOType.OUTPUT.getCode());
            if (!processDefinitionIOS.isEmpty()) {
                for (ProcessDefinitionIO processDefinitionIO : processDefinitionIOS) {
                    String ioName = processDefinitionIO.getIoName();
                    ioNames.add(ioName);

                    // 下游血缘中是否存在已有 ”输入表“
                    if (inputIoName.contains(ioName)) {
                        throw new SQLIOException(String.format("通过 SQL 建立的的表中含有循环依赖的情况, 下游血缘输出表 %s 与工作流定义输入表相同",ioName));
                    }
                }
            }
            // 清空上次循环查询结果
            ids.clear();
        }
    }
}
