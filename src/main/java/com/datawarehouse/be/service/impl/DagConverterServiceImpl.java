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
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.datawarehouse.be.configuration.SensorConfig;
import com.datawarehouse.be.constants.ProcessConstant;
import com.datawarehouse.be.pojo.dto.*;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.common.enums.IOType;
import com.datawarehouse.be.enums.ProcessParamsGenericEnum;
import com.datawarehouse.be.exceptions.AirflowDAGParamsException;
import com.datawarehouse.be.exceptions.AirflowTaskIsEmptyException;
import com.datawarehouse.be.exceptions.ProcessConvertException;
import com.datawarehouse.be.pojo.dos.CreateScheduleDO;
import com.datawarehouse.be.pojo.dos.ProcessAddDO;
import com.datawarehouse.be.pojo.dos.ProcessJsonDO;
import com.datawarehouse.be.pojo.params.AirflowDAGAddParam;
import com.datawarehouse.be.service.DagConverterService;
import com.datawarehouse.be.service.HiveMetaService;
import org.apache.dolphinscheduler.api.dto.ScheduleParam;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.ProcessDefinitionService;
import org.apache.dolphinscheduler.api.service.SchedulerService;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.FailureStrategy;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.common.utils.CollectionUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;

import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.Resource;
import org.apache.dolphinscheduler.dao.entity.Schedule;
import org.apache.dolphinscheduler.dao.entity.User;

import org.apache.dolphinscheduler.dao.entity.ProcessDefinitionIO;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.ResourceMapper;
import org.apache.dolphinscheduler.dao.mapper.ScheduleMapper;
import org.apache.dolphinscheduler.dao.mapper.UserMapper;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionIOMapper;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.apache.dolphinscheduler.spi.utils.StringUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class DagConverterServiceImpl implements DagConverterService {

    private static final Logger logger = LoggerFactory.getLogger(DagConverterServiceImpl.class);

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private ProcessDefinitionMapper processDefinitionMapper;

    @Autowired
    private ProcessDefinitionService processDefinitionService;

    @Autowired
    private SchedulerService schedulerService;

    @Autowired
    private ProjectService projectService;

    /**
     * 分区探测配置
     */
    @Autowired
    private SensorConfig sensorConfig;

    @Autowired
    private ScheduleMapper scheduleMapper;

    @Autowired
    private ProcessDefinitionIOMapper processDefinitionIOMapper;

    @Autowired
    private HiveMetaService hiveMetaService;

    @Autowired
    private ResourceMapper resourcesMapper;

    /**
     * 处理逻辑：
     *   1.获取当前用户
     *   2.转换dag为工作流
     *   3.生成工作流的“连接线”
     *   4.生成工作流的“坐标”
     *   5.生成/修改工作流
     *   6.上线工作流
     *   7.生成/更新工作流调度
     *   8.上线工作流调度
     */
    @Transactional(rollbackFor = Exception.class)
    @Override
    public Map<String, Object> dagConverter(AirflowDAGAddParam airflowDAGAddParam) {
        // 1.获取当前用户
        User user = userMapper.queryByUserNameAccurately(airflowDAGAddParam.getCreator());

        // 2.转换D平台的json串为ds的工作流定义：做一个t_ds_process_definition写数据库的操作
        ProcessAddDO processAddDO = ((DagConverterServiceImpl) SpringApplicationContext.getBean(DagConverterService.class)).processJsonDOConverter(airflowDAGAddParam, user);

        // 3.生成工作流的“连接线”
        JsonArray connects = generateConnects(processAddDO);

        // 4.生成工作流的“坐标”
        String locations = generateLocations(processAddDO);

        // 5.生成\修改工作流
        Map<String, Object> createProcessResult = null;
        Long processId = airflowDAGAddParam.getProcessId();
        Gson gson = new GsonBuilder().serializeNulls().create();

        Long projectCode = processAddDO.getProjectCode();
        if (processId == null) {
            createProcessResult = processDefinitionService.createProcessDefinition(user,
                    projectCode,
                    processAddDO.getName(),
                    processAddDO.getDescription(),
                    null,
                    locations,
                    1,
                    "hive",
                    gson.toJson(connects),
                    gson.toJson(processAddDO.getProcessJsonDO().getTasksDTO())
            );
            ProcessDefinition processDefinition = (ProcessDefinition) createProcessResult.get("data");
            processId = processDefinition.getCode();

        }else {
            // 下线工作调度
            Map<String, Object> offLineResult = processDefinitionService.
                    releaseProcessDefinition(
                            user,
                            projectCode,
                            processId,
                            ReleaseState.OFFLINE);
            // 更新
            createProcessResult = processDefinitionService.
                    updateProcessDefinition(
                            user,
                            projectCode,
                            processAddDO.getName(),
                            processId,
                            processAddDO.getDescription(),
                            null,
                            locations,
                            1,
                        "hive",
                            gson.toJson(connects),
                            gson.toJson(processAddDO.getProcessJsonDO().getTasksDTO()));
            if (Optional.ofNullable(createProcessResult).map(e -> !Objects.equals(Status.SUCCESS, e.get(Constants.STATUS))).orElse(true)) {
                throw new ProcessConvertException(String.format("更新工作流失败，详情：%s", JSON.toJSONString(createProcessResult)));
            }
        }
        Map<String, Object> releaseProcessResult = null;
        // 上线
        releaseProcessResult = processDefinitionService.releaseProcessDefinition(
                user, projectCode, processId, ReleaseState.ONLINE);

        // 7.保存/更新工作流调度
        String processName = airflowDAGAddParam.getName();
        Map<String, Object> createScheduleResult = null;
        Integer scheduleId = null;
        if (airflowDAGAddParam.getProcessId() == null) {
            CreateScheduleDO createScheduleDO = createProcessSchedule(
                    airflowDAGAddParam,
                    processId,
                    processName);
            createScheduleResult = schedulerService.insertSchedule(
                    user,
                    projectCode,
                    processId,
                    createScheduleDO.getSchedule(),
                    createScheduleDO.getWarningType(),
                    createScheduleDO.getWarningGroupId(),
                    createScheduleDO.getFailureStrategy(),
                    createScheduleDO.getProcessInstancePriority(),
                    createScheduleDO.getWorkerGroup(),
                -1L
            );
            scheduleId = (int) createScheduleResult.get("scheduleId");
        } else {
            Schedule schedule = scheduleMapper.queryByProcessDefinitionCode(processId);
            if (schedule != null){
                ScheduleParam scheduleParam = new ScheduleParam(schedule.getStartTime(), schedule.getEndTime(), null, airflowDAGAddParam.getCron());
                createScheduleResult = schedulerService.updateSchedule(
                        user
                        , projectCode
                        , schedule.getId()
                        , JSON.toJSONString(scheduleParam)
                        , schedule.getWarningType()
                        , schedule.getWarningGroupId()
                        , schedule.getFailureStrategy()
                        , schedule.getProcessInstancePriority()
                        , schedule.getWorkerGroup()
                        ,-1L);
                scheduleId = schedule.getId();

                // 8.上线工作流调度
                Map<String, Object> schedulerOnlineResult = null;
                try {
                    schedulerOnlineResult = schedulerService.setScheduleState(
                            user,
                            projectCode,
                            scheduleId,
                            ReleaseState.ONLINE);
                } catch (Exception e) {
                    e.printStackTrace();
                    new ProcessConvertException(String.format("上线工作流调度异常，详情：%s", e.toString()));
                }
                if (Optional.ofNullable(schedulerOnlineResult).map(e -> !Objects.equals(Status.SUCCESS, e.get(Constants.STATUS))).orElse(true)) {
                    throw new ProcessConvertException(String.format("上线工作流调度失败，详情：%s", JSON.toJSONString(schedulerOnlineResult)));
                }
            }

        }
        Map<String, Object> result = new HashMap<>(2);
        result.put(Constants.STATUS, Status.SUCCESS);
        result.put(ProcessConstant.PROCESSDEFINITIONID, processId);
        return result;
    }

    private CreateScheduleDO createProcessSchedule(AirflowDAGAddParam airflowDAGAddParam, Long processDefinitionId, String processDefinitionName) {
        CreateScheduleDO createScheduleDO = new CreateScheduleDO();
        String cronTab = Optional.ofNullable(airflowDAGAddParam).map(AirflowDAGAddParam::getCron).filter(e -> e.trim().length() == 9).map(e -> new StringBuffer("0 ").append(e).substring(0, e.length()) + " ? *").orElse(airflowDAGAddParam.getCron());
        String startDate = Optional.ofNullable(airflowDAGAddParam).map(AirflowDAGAddParam::getStartDate).filter(e -> e.length() <= 10).map(e -> e + " 00:00:00").orElse(airflowDAGAddParam.getStartDate());
        DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.forID("Asia/Shanghai"));
        ScheduleParam scheduleObj = new ScheduleParam();
        scheduleObj.setCrontab(cronTab);
        scheduleObj.setStartTime(dateTimeFormatter.parseDateTime(startDate).toDate());
        scheduleObj.setEndTime(dateTimeFormatter.parseDateTime("2999-12-31 00:00:00").toDate());
        createScheduleDO.setSchedule(JSONUtils.toJsonString(scheduleObj));
        createScheduleDO.setWarningGroupId(1);
        createScheduleDO.setWorkerGroup("default");
        createScheduleDO.setProcessDefineId(processDefinitionId);
        createScheduleDO.setWarningType(WarningType.FAILURE);
        createScheduleDO.setProcessInstancePriority(Priority.MEDIUM);
        createScheduleDO.setFailureStrategy(FailureStrategy.CONTINUE);
        return createScheduleDO;
    }

    @Transactional(rollbackFor = Exception.class)
    public ProcessAddDO processJsonDOConverter(AirflowDAGAddParam airflowDAGAddParam, User user){
        ProcessAddDO processAddDO = new ProcessAddDO();
        processAddDO.wrap(airflowDAGAddParam.getProjectCode(), airflowDAGAddParam.getName(), airflowDAGAddParam.getDescription(), airflowDAGAddParam.getCreator());
        ProcessJsonDODTO dagProcessJsonDO = Optional.ofNullable(airflowDAGAddParam.getProcessJsonDO()).orElseThrow(() -> new AirflowTaskIsEmptyException("Dag不能为空，请确认~"));
        ProcessJsonDO processJsonDO = new ProcessJsonDO();
        processJsonDO.init();
        processJsonDO.wrap(dagProcessJsonDO.getTenantId());
        List<TasksDTO> tasks = Optional.ofNullable(airflowDAGAddParam.getProcessJsonDO()).map(ProcessJsonDODTO::getTasks).orElseThrow(() -> new AirflowTaskIsEmptyException("Dag中task集合不能为空，请确认~"));
        ((DagConverterServiceImpl) SpringApplicationContext.getBean(DagConverterService.class)).generateDepOfProcessTask(tasks, user);
        processJsonDO.setTasksDTO(tasks);
        processAddDO.setProcessJsonDO(processJsonDO);
        return processAddDO;
    }

    private List<ProcessDefinitionIO> getOutputTables() {
        List<ProcessDefinitionIO> outputTables = processDefinitionIOMapper.queryByIOType(IOType.OUTPUT.getCode());
        if (CollectionUtils.isNotEmpty(outputTables)) {
            for (ProcessDefinitionIO io : outputTables) {
                if (io != null && StringUtils.isNotBlank(io.getIoName())) {
                    io.setIoName(new StringBuffer(io.getIoName()).append(".$[yyyyMMdd-1].sensor").toString());
                }
            }
        }
        return outputTables;
    }

    public void generateDepOfProcessTask(List<TasksDTO> tasks, User user) {
        for (TasksDTO task : tasks) {
            if (task.getTaskType() ==  "DEPENDENT") {
                String depName = task.getName();
                ProcessDefinition processDefinition = processDefinitionMapper.queryByDefineName(sensorConfig.projectId, depName);
                String[] depNameArray = Optional.ofNullable(depName).map(e -> e.split("\\.")).orElseThrow(() -> new AirflowDAGParamsException(String.format("入参depTasks的格式必须为:库名.表名.分区名.sensor,当前值为：%s，不符合要求，请确认~", depName)));
                logger.info("生产一个依赖任务元数据列表：" + Arrays.stream(depNameArray).collect(Collectors.toList()));
                if (depNameArray.length < 4) {
                    throw new AirflowDAGParamsException(String.format("入参depTasks的格式必须为:库名.表名.分区名.sensor,当前值为：%s，不符合要求，请确认~", depName));
                }
                String dbName = depNameArray[0];
                String tableName = depNameArray[1];
                String pythonShell = String.format("#_*_coding:utf-8_*_ \nimport partition_sensor.hive_partition_sensor as partition_sensor \npartition_sensor.check_for_partition('%s', '%s', '${partition}=\\'${format}\\'')", dbName, tableName);
                logger.info("通过元数据列表拼接生成python脚本：" + pythonShell);
                LocalParamsDTO formatLocalParamsDTO = new LocalParamsDTO();
                LocalParamsDTO partitionLocalParamsDTO = new LocalParamsDTO();
                ResourceListDTO resourceListDTO = new ResourceListDTO();
                logger.info("生成依赖工作流时分区字段的判定");
                Map<String, String> dtMap = hiveMetaService.checkPartition(dbName, tableName);
                logger.info("完成-生成依赖工作流时分区字段的判定");
                if (dtMap.isEmpty()) {
                    throw new ServiceException(String.format("请检查 %s.%s 单个分区并非按天进行分区，请联系研发解决", dbName, tableName));
                }
                formatLocalParamsDTO.setProp("format");
                formatLocalParamsDTO.setValue(dtMap.get(HiveMetaService.FORMAT));
                partitionLocalParamsDTO.setProp("partition");
                partitionLocalParamsDTO.setValue(dtMap.get(HiveMetaService.PARTITION));
                List<Resource> resources = resourcesMapper.queryResource("/partition_sensor/hive_partition_sensor.py", 0);
                resourceListDTO.setId(resources.get(0).getId());
                DependenceTypeParams taskParams = (DependenceTypeParams)task.getTaskParams();
                if (processDefinition == null) {
                    logger.info(depName + "依赖节点的,工作流没有被定义");
                    logger.info("执行-创建基础依赖节点的工作流定义");
                    logger.info("生成依赖任务：{}", depName);
                    ProcessJsonDO processJsonDO = new ProcessJsonDO();
                    PythonTypeParams pythonTypeParams = new PythonTypeParams();
                    pythonTypeParams.setRawScript(pythonShell);
                    pythonTypeParams.setLocalParams(Lists.newArrayList(formatLocalParamsDTO, partitionLocalParamsDTO));
                    pythonTypeParams.setResourceList(Lists.newArrayList(resourceListDTO));
                    TasksDTO pythonTask;
                    String jsonString = JSONUtils.toJsonString(task);
                    pythonTask = JSONUtils.parseObject(jsonString, TasksDTO.class);
                    pythonTask.setCode(new Random().nextInt(99999 + 1));
                    pythonTask.setTaskType("PYTHON");
                    pythonTask.setTaskParams(pythonTypeParams);
                    processJsonDO.setTasksDTO(Collections.singletonList(pythonTask));
                    Gson gson = new GsonBuilder().serializeNulls().create();
                    logger.info("在common 下就成功创建新的探分区的工作流");
                    Map<String, Object> res = processDefinitionService.createProcessDefinition(user,
                        sensorConfig.projectId,
                        depName,
                        null,
                        "[]",
                        "[{\"taskCode\":" + pythonTask.getCode() + ",\"x\":229,\"y\":223}]",
                        0,
                        "hive",
                        "[{\"name\":\"\",\"preTaskCode\":0,\"preTaskVersion\":0,\"postTaskCode\":" + pythonTask.getCode() + ",\"postTaskVersion\":0,\"conditionType\":0,\"conditionParams\":{}}]",
                        gson.toJson(Lists.newArrayList(pythonTask))
                    );
                    processDefinition = (ProcessDefinition)res.get("data");
                    logger.info("上线工作流定义");
                    Map<String, Object> onLineResult = processDefinitionService.releaseProcessDefinition(user, processDefinition.getProjectCode(), processDefinition.getCode(), ReleaseState.ONLINE);
                    boolean whetherOnLine = Optional.ofNullable(onLineResult).map(e -> Objects.equals(Status.SUCCESS, e.get(Constants.STATUS))).orElse(true);
                    if (!whetherOnLine) {
                        throw new ProcessConvertException(String.format("新建依赖任务时，上线默认工作流(processName:%s,processId:%s)失败，请查看日志!", processDefinition.getProjectName(), processDefinition.getId()));
                    }
                    try {
                        schedulerService.insertSchedule(user,
                            processDefinition.getProjectCode(),
                            processDefinition.getCode(),
                            "{\"startTime\":\"2022-09-13 00:00:00\",\"endTime\":\"2122-09-13 00:00:00\",\"crontab\":\"0 50 2 * * ? *\"}",
                            WarningType.NONE,
                            0,
                            FailureStrategy.CONTINUE,
                            Priority.MEDIUM,
                            "default",
                            -1L);
                    } catch (Exception e) {
                        throw new ProcessConvertException(String.format("新建依赖任务时，上线工作流调度(processName:%s,processId:%s)失败，请查看日志!", processDefinition.getProjectName(), processDefinition.getId()));
                    }
                    try {
                        List<Schedule> scheduleList = scheduleMapper.queryReleaseSchedulerListByProcessDefinitionCode(processDefinition.getCode());
                        if (CollectionUtils.isEmpty(scheduleList)) {
                            throw new ProcessConvertException(String.format("获取任务的调度列表为空，请检查generateDepOfProcessTask方法380行"));
                        }
                        for (Schedule schedule : scheduleList) {
                            logger.info("上线工作流调度任务");
                            Map<String, Object> schedulerOnlineResult = schedulerService.setScheduleState(user, processDefinition.getCode(), schedule.getId(), ReleaseState.ONLINE);
                            if (Optional.ofNullable(schedulerOnlineResult).map(e -> !Objects.equals(Status.SUCCESS, e.get(Constants.STATUS))).orElse(true)) {
                                throw new ProcessConvertException(String.format("上线工作流调度失败，详情：%s", JSON.toJSONString(schedulerOnlineResult)));
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        new ProcessConvertException(String.format("上线工作流调度异常，详情：%s", e));
                    }
                }
                long code = processDefinition.getCode();
                long projectCode = processDefinition.getProjectCode();
                taskParams.getDependence().getDependTaskList().get(0).getDependItemList().get(0).setProjectCode(projectCode);
                taskParams.getDependence().getDependTaskList().get(0).getDependItemList().get(0).setDefinitionCode(code);
                taskParams.setRawScript(pythonShell);
                taskParams.setLocalParams(Lists.newArrayList(formatLocalParamsDTO, partitionLocalParamsDTO));
                taskParams.setConditionResult(new ConditionResultDTO());
                taskParams.setResourceList(Lists.newArrayList(resourceListDTO));
                task.setTaskType("DEPENDENT");
                task.setTaskParams(taskParams);
            }
            }
        }

    @Override
    public TasksDTO getDependentTask(ProcessDefinition processDefinition, String depName) {
        //String processJson = Optional.ofNullable(processDefinition).map(ZiroomProcessDefinition::getProcessDefinitionJson).orElse(null);
        String processJson="111";
        if (processJson == null) {
            return null;
        }
        ProcessJsonDO processJsonDO = parseProcessJson(processJson);
        List<TasksDTO> tasks = Optional.ofNullable(processJsonDO).map(ProcessJsonDO::getTasksDTO).orElseThrow(null);
        if (CollectionUtils.isEmpty(tasks)) {
            return null;
        }
        TasksDTO tasksDTO = tasks.stream().filter(e -> StringUtils.equalsIgnoreCase(depName, e.getName())).filter(e -> e != null).findFirst().orElse(null);
        return tasksDTO;
    }

    public ProcessJsonDO parseProcessJson(String processJson) {
        Gson gson = new GsonBuilder().serializeNulls().create();
        ProcessJsonDO processJsonDO = gson.fromJson(processJson, ProcessJsonDO.class);
        List<TasksDTO> tasks = Optional.ofNullable(processJsonDO).map(ProcessJsonDO::getTasksDTO).orElseThrow(null);
        if (CollectionUtils.isEmpty(tasks)) {
            return null;
        }
        for (TasksDTO item : tasks) {
            String type = item.getTaskType();
            if ("DEPENDENT".equals(type)) {
                continue;
            }
            Class processParamsClazz = Optional.ofNullable(ProcessParamsGenericEnum.getProcessParamsClazz(type)).orElseThrow(() -> new AirflowDAGParamsException(String.format("未匹配类型为：%s，对应的工作流入参泛型实例", type)));
            Object paramsJsonClass = gson.fromJson(gson.toJson(item.getTaskParams()), processParamsClazz);
            item.setTaskParams(paramsJsonClass);
        }
        return processJsonDO;
    }

    public String generateLocations(ProcessAddDO processAddDO) {
        List<TasksDTO> tasks = Optional.ofNullable(processAddDO).map(ProcessAddDO::getProcessJsonDO).map(ProcessJsonDO::getTasksDTO).orElseThrow(() -> new AirflowTaskIsEmptyException("Dag中task集合不能为空，请确认~"));
        String result = "";
        for (int i = 0, tasksSize = tasks.size(); i < tasksSize; i++) {
            TasksDTO task = tasks.get(i);
            String locationKey = String.valueOf(task.getCode());
            result += String.format("{\"taskCode\":%s,\"x\":%s,\"y\":%s},",locationKey,200 + 200 * i,50 + 150 * i);
        }
        result = result.substring(0,result.length() - 1);
        result = "["+result+"]";
        return result;
    }

    public JsonArray generateConnects(ProcessAddDO processAddDO) {
        // 返回结果
        JsonArray result = new JsonArray();
        ArrayList<TasksDTO> dpTaskLists = new ArrayList<>();
        ArrayList<TasksDTO> hqlTaskLists = new ArrayList<>();
        // 获取任务集合
        List<TasksDTO> tasks = Optional.ofNullable(processAddDO).map(ProcessAddDO::getProcessJsonDO).map(ProcessJsonDO::getTasksDTO).orElseThrow(() -> new AirflowTaskIsEmptyException("Dag中task集合不能为空，请确认~"));

        for (TasksDTO task : tasks) {
            if (Objects.equals(task.getTaskType(), "DEPENDENT")){
                JsonObject connectObject = new JsonObject();
                Integer code = task.getCode();
                connectObject.addProperty("name","");
                connectObject.addProperty("preTaskCode", 0);
                connectObject.addProperty("preTaskVersion",0);
                connectObject.addProperty("postTaskCode", code);
                connectObject.addProperty("postTaskVersion", 0);
                connectObject.addProperty("conditionType", 0);
                connectObject.add("conditionParams",new JsonObject());
                result.add(connectObject);
                dpTaskLists.add(task);
            }
        }

        for (TasksDTO task : tasks) {
            if (Objects.equals(task.getTaskType(), "SQL")){
                Integer postTaskCode = task.getCode();
                for (TasksDTO dpTask : dpTaskLists) {
                    JsonObject connectObject = new JsonObject();
                    Integer preTaskCode = dpTask.getCode();
                    connectObject.addProperty("name","");
                    connectObject.addProperty("preTaskCode", preTaskCode);
                    connectObject.addProperty("preTaskVersion",0);
                    connectObject.addProperty("postTaskCode", postTaskCode);
                    connectObject.addProperty("postTaskVersion", 0);
                    connectObject.addProperty("conditionType", 0);
                    connectObject.add("conditionParams",new JsonObject());
                    result.add(connectObject);
                }
                hqlTaskLists.add(task);
            }
        }
        for (TasksDTO task : tasks) {
            if (Objects.equals(task.getTaskType(), "PYTHON")){
                Integer postTaskCode = task.getCode();
                if (hqlTaskLists.isEmpty()){
                    JsonObject connectObject = new JsonObject();
                    Integer code = task.getCode();
                    connectObject.addProperty("name","");
                    connectObject.addProperty("preTaskCode", 0);
                    connectObject.addProperty("preTaskVersion",0);
                    connectObject.addProperty("postTaskCode", code);
                    connectObject.addProperty("postTaskVersion", 0);
                    connectObject.addProperty("conditionType", 0);
                    connectObject.add("conditionParams",new JsonObject());
                    result.add(connectObject);
                }else{
                    for (TasksDTO hqlTask : hqlTaskLists) {
                        JsonObject connectObject = new JsonObject();
                        Integer preTaskCode = hqlTask.getCode();
                        connectObject.addProperty("name","");
                        connectObject.addProperty("preTaskCode", preTaskCode);
                        connectObject.addProperty("preTaskVersion",0);
                        connectObject.addProperty("postTaskCode", postTaskCode);
                        connectObject.addProperty("postTaskVersion", 0);
                        connectObject.addProperty("conditionType", 0);
                        connectObject.add("conditionParams",new JsonObject());
                        result.add(connectObject);
                    }
                }
            }
        }
        return result;
    }
}

