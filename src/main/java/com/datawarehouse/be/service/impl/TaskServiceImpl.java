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

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.datawarehouse.be.configuration.SensorConfig;
import com.datawarehouse.be.pojo.vo.ProcessStateVo;
import com.datawarehouse.be.service.TaskService;
import lombok.SneakyThrows;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.ExecutorService;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.api.service.UsersService;
import org.apache.dolphinscheduler.api.service.impl.BaseServiceImpl;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.enums.FailureStrategy;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.RunMode;
import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.common.enums.TaskType;
import org.apache.dolphinscheduler.common.enums.UserType;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.common.model.DependentItem;
import org.apache.dolphinscheduler.common.model.DependentTaskModel;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.task.dependent.DependentParameters;
import org.apache.dolphinscheduler.common.utils.CollectionUtils;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;

import org.apache.dolphinscheduler.dao.entity.*;
import org.apache.dolphinscheduler.dao.mapper.*;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class TaskServiceImpl extends BaseServiceImpl implements TaskService {
    private static final Logger logger = LoggerFactory.getLogger(TaskServiceImpl.class);

    @Autowired
    private TaskMapper taskMapper;

    @Autowired
    private ZiroomTodayTaskMapper ziroomTodayTaskMapper;

    @Autowired
    private ProcessDefinitionMapper ProcessDefinitionMapper;

    @Autowired
    private ProcessService processService;

    @Autowired
    private SensorConfig sensorConfig;

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private ProcessDefinitionMapper processDefinitionMapper;

    @Autowired
    private ProcessInstanceMapper processInstanceMapper;

    @Autowired
    UsersService usersService;

    @Autowired
    ProjectMapper projectMapper;

    @Autowired
    ProjectService projectService;

    @Autowired
    ProcessInstanceMapper dwprocessInstanceMapper;

    @Autowired
    private ExecutorService execService;

    public Map<String,Object> queryTaskListPaging(User loginUser,
                                                  String createUserName, String searchVal,
                                                  Integer pageNo, Integer pageSize) {
        Map<String, Object> result = new HashMap<>(5);
        Page<Task> page = new Page(pageNo, pageSize);
        PageInfo pageInfo = new PageInfo<TaskInstance>(pageNo, pageSize);

        int loginUserId = loginUser.getUserType() == UserType.ADMIN_USER ? 0 : loginUser.getId();

        IPage<Task> ziroomTaskIPage =  taskMapper.queryTaskListPaging(page, searchVal, loginUserId, createUserName);

        Set<String> exclusionSet = new HashSet<>();
        exclusionSet.add(Constants.CLASS);
        exclusionSet.add("taskJson");
        List<Task> taskInstanceList = ziroomTaskIPage.getRecords();

        pageInfo.setTotal((int)ziroomTaskIPage.getTotal());
        pageInfo.setTotalList(CollectionUtils.getListByExclusion(taskInstanceList,exclusionSet));
        result.put(Constants.DATA_LIST, pageInfo);
        putMsg(result, Status.SUCCESS);

        return result;
    }

    public Map<String,Object> queryTodaySchedulerInfo(User loginUser){
        Map<String, Object> result = new HashMap<>(5);
        int loginUserId = loginUser.getUserType() == UserType.ADMIN_USER ? 0 : loginUser.getId();
        TodaySchedulerInfo todaySchedulerInfo = ziroomTodayTaskMapper.queryTodaySchedulerInfo(loginUserId);
        result.put(Constants.DATA_LIST, todaySchedulerInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    public Map<String, Object> queryProcessInstanceList(User loginUser, String projectName, Long processDefineId,
                                                        String startTime,String endTime,String stateVal,String executorName,
                                                        Integer pageNo, Integer pageSize) {

        Map<String, Object> result = new HashMap<>(5);
        Project project = projectMapper.queryByName(projectName);

        Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, project.getCode());
        Status resultEnum = (Status) checkResult.get(Constants.STATUS);
        if (resultEnum != Status.SUCCESS) {
            return checkResult;
        }

        Map<String, Object> checkAndParseDateResult = checkAndParseDateParameters(startTime, endTime);
        if (checkAndParseDateResult.get(Constants.STATUS) != Status.SUCCESS) {
            return checkAndParseDateResult;
        }
        Date start = (Date) checkAndParseDateResult.get(Constants.START_TIME);
        Date end = (Date) checkAndParseDateResult.get(Constants.END_TIME);

        Page<ProcessInstance> page = new Page(pageNo, pageSize);
        PageInfo pageInfo = new PageInfo<ProcessInstance>(pageNo, pageSize);
        int executorId = usersService.getUserIdByName(executorName);


        IPage<ProcessInstance> processInstanceList =
            dwprocessInstanceMapper.queryProcessInstanceListPaging(page,
                project.getId(),stateVal,processDefineId,executorId,start,end);

        List<ProcessInstance> processInstances = processInstanceList.getRecords();

        for(ProcessInstance processInstance: processInstances){
            String [] lineStates = new String[]{
                ExecutionStatus.RUNNING_EXECUTION.getDescp(),
                ExecutionStatus.SUBMITTED_SUCCESS.getDescp(),
                ExecutionStatus.NEED_FAULT_TOLERANCE.getDescp(),
                ExecutionStatus.WAITING_THREAD.getDescp(),
                ExecutionStatus.WAITING_DEPEND.getDescp()
            };
            ExecutionStatus state = processInstance.getState();
            if(state == null || Arrays.asList(lineStates).contains(state.getDescp())){
                processInstance.setDuration("--");
            } else{
                processInstance.setDuration(DateUtils.format2Duration(processInstance.getStartDateTime(),processInstance.getEndDateTime()));
            }
        }
        pageInfo.setPageSize((int) processInstanceList.getTotal());
        pageInfo.setTotalList(processInstances);
        result.put(Constants.DATA_LIST, pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public void execHivePartitionSensorAfterUpdate() {
        Date today = new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(today);

        calendar.set(Calendar.HOUR_OF_DAY,0);
        calendar.set(Calendar.MINUTE,0);
        calendar.set(Calendar.SECOND,0);
        Date startTime = calendar.getTime();

        calendar.set(Calendar.HOUR_OF_DAY,23);
        calendar.set(Calendar.MINUTE,59);
        calendar.set(Calendar.SECOND,59);
        Date endTime = calendar.getTime();

        ProcessInstance processInstance = processService.findLastRunningProcess(sensorConfig.processId, startTime, endTime);
        ProcessDefinition processDefinition = ProcessDefinitionMapper.selectById(sensorConfig.processId);

        // 当前没有正在执行的探分区工作流
        if (processInstance == null) {
            logger.info("探分区工作流, processId: {} 无任何正在执行的工作流实例", sensorConfig.processId);
            //execProcessOfHivePartitionSensor();
        } else if (processDefinition.getUpdateTime().compareTo(processInstance.getStartTime()) == 1 ) {
            // 停止正在执行的工作流实例
            String projectName = sensorConfig.projectName;
            String projectOwnerName = sensorConfig.projectOwner;
            User user = userMapper.queryByUserNameAccurately(projectOwnerName);
            //TODO:貌似方法发生变先注释了
            //logger.info("停止执行探分区工作流实例:processInstanceId:{}, 状态：{}",processInstance.getId(), execService.execute(user, projectName, processInstance.getId(), ExecuteType.STOP).get("status"));

            // 执行最新的工作流
            //execProcessOfHivePartitionSensor();
            logger.info("执行更新后的探分区工作流成功");

        } else {
            logger.info("探分区工作流, processId: {} 最近未被更新", sensorConfig.processId);
        }
    }

    public Map<String, Object> complementProcessInstanceList(User loginUser, String projectName, long processDefineId,
                                                             String startTime,String endTime,String stateVal,String executorName,
                                                             Integer  startorder, Integer  endorder,
                                                             Integer  runorder, Integer  taskorder,
                                                             Integer pageNo, Integer pageSize) {

        Map<String, Object> result = new HashMap<>(5);
        Project project = projectMapper.queryByName(projectName);

        Map<String, Object> checkResult = projectService.checkProjectAndAuth(loginUser, project, project.getCode());
        Status resultEnum = (Status) checkResult.get(Constants.STATUS);
        if (resultEnum != Status.SUCCESS) {
            return checkResult;
        }

        Map<String, Object> checkAndParseDateResult = checkAndParseDateParameters(startTime, endTime);
        if (checkAndParseDateResult.get(Constants.STATUS) != Status.SUCCESS) {
            return checkAndParseDateResult;
        }
        Date start = (Date) checkAndParseDateResult.get(Constants.START_TIME);
        Date end = (Date) checkAndParseDateResult.get(Constants.END_TIME);

        Page<ProcessInstance> page = new Page(pageNo, pageSize);
        PageInfo pageInfo = new PageInfo<ProcessInstance>(pageNo, pageSize);

        int executorId = usersService.getUserIdByName(executorName);
        IPage<ProcessInstance> processInstanceList =
            dwprocessInstanceMapper.complementProcessInstanceListPaging(page,
                project.getId(),stateVal,processDefineId,executorId,start,end,startorder,endorder,taskorder,runorder);
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<ProcessInstance> processInstances = processInstanceList.getRecords();
        processInstances=processInstanceList.getRecords().stream().map(pro->{
            Date schtime=null;
            try {
                schtime = simpleDateFormat.parse(pro.getScheduleDateTime());
            } catch (ParseException e) {
                e.printStackTrace();
            }
            calendar.setTime(schtime);
            calendar.add(Calendar.HOUR,8);
            Date time = calendar.getTime();
            String format = simpleDateFormat.format(time);
            String[] s = format.toString().split(" ");
            String stime=s[0]+" 00:00:00";
            pro.setScheduleDateTime(stime);
            return pro;
        }).collect(Collectors.toList());
        for(ProcessInstance processInstance: processInstances){
            //时长需要做特殊处理的状态
            String [] lineStates = new String[]{
                ExecutionStatus.RUNNING_EXECUTION.getDescp(),
                ExecutionStatus.SUBMITTED_SUCCESS.getDescp(),
                ExecutionStatus.NEED_FAULT_TOLERANCE.getDescp(),
                ExecutionStatus.WAITING_THREAD.getDescp(),
                ExecutionStatus.WAITING_DEPEND.getDescp()
            };
            ExecutionStatus state = processInstance.getState();
            if(state == null || Arrays.asList(lineStates).contains(state.getDescp())){
                processInstance.setDuration("--");
            } else{
                processInstance.setDuration(DateUtils.format2Duration(processInstance.getStartDateTime(),processInstance.getEndDateTime()));
            }
        }
        pageInfo.setPageSize((int) processInstanceList.getTotal());
        pageInfo.setTotalList(processInstances);
        result.put(Constants.DATA_LIST, pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    @SneakyThrows
    public void execDependentTaskWhenComplement(User loginUser, Long projectCode,
                                                long processDefinitionId, String cronTime, CommandType commandType,
                                                FailureStrategy failureStrategy, String startNodeList,
                                                TaskDependType taskDependType, WarningType warningType, int warningGroupId,
                                                String receivers, String receiversCc, RunMode runMode,
                                                Priority processInstancePriority, String workerGroup, Integer timeout) {
        ProcessDefinition processDefinition = processDefinitionMapper.queryByDefineId(processDefinitionId);
        long processDefinitionCode = processDefinition.getCode();
        List<ProcessTaskRelation> processTaskRelations = processService.findRelationByCode(processDefinitionCode, processDefinition.getVersion());
        List<TaskDefinitionLog> taskDefinitionLogs = processService.getTaskDefineLogListByRelation(processTaskRelations);
        List<TaskNode> taskNodes = processService.transformTask(processTaskRelations, taskDefinitionLogs);
        for (TaskNode task : taskNodes) {
            if (task.getType().equals(String.valueOf(TaskType.DEPENDENT))) {
                Multimap<String, Long> multiMap = ArrayListMultimap.create();
                Long dependentProcessDefinitionId = Long.parseLong(null);
                String dependentProjectName = null;
                ProcessDefinition dependentProcessDefinition = processDefinitionMapper.queryByDefineName(sensorConfig.projectId, task.getName());
                if (Optional.ofNullable(dependentProcessDefinition).isPresent()) {
                    dependentProcessDefinitionId = dependentProcessDefinition.getCode();
                    dependentProjectName = sensorConfig.projectName;
                } else {
                    DependentParameters dependentParameters = JSONUtils.parseObject(task.getDependence(), DependentParameters.class);
                    if (Optional.ofNullable(dependentParameters).isPresent()) {
                        List<DependentTaskModel> taskModels = dependentParameters.getDependTaskList();
                        for (DependentTaskModel dependentTaskModel : taskModels) {
                            List<DependentItem> dependentItemList = dependentTaskModel.getDependItemList();
                            for (DependentItem dependentItem : dependentItemList) {
                                dependentProcessDefinitionId = dependentItem.getDefinitionCode();
                                Project project = projectMapper.queryDetailById(dependentItem.getProjectCode());
                                dependentProjectName = project.getName();
                            }
                        }
                    }
                }
                String[] dateList = cronTime.split(",");
                Date start = DateUtils.parse(dateList[0], "yyyy-MM-dd HH:mm:ss");
                Date end = DateUtil.offset(DateUtils.parse(dateList[1], "yyyy-MM-dd HH:mm:ss"), DateField.DAY_OF_MONTH, 1);
                long between = DateUtil.between(start, end, DateUnit.DAY);
                boolean flag = true;
                for (int i = 0; i < (int) between; i++) {
                    Date startTime = DateUtil.offset(start, DateField.DAY_OF_MONTH, i);
                    Date endTime = DateUtil.offset(start, DateField.DAY_OF_MONTH, i+1);
                    List<ProcessInstance> processInstanceList = processInstanceMapper.queryLastManualProcessList(
                        dependentProcessDefinitionId,
                        startTime,
                        endTime
                    );
                    if (processInstanceList.isEmpty()) {
                        multiMap.put(startTime+ "," + startTime, dependentProcessDefinitionId);
                    } else {
                        for (ProcessInstance instance : processInstanceList) {
                            if (Optional.ofNullable(instance).isPresent()) {
                                if (!instance.getState().equals(ExecutionStatus.SUCCESS)) {
                                    processInstanceMapper.deleteById(instance.getId());
                                } else {
                                    flag = false;
                                }
                            }
                        }
                        if (flag) {
                            String date = DateUtil.format(processInstanceList.get(0).getStartTime(), "yyyy-MM-dd");
                            multiMap.put(date + " 00:00:00," + date + " 00:00:00", dependentProcessDefinitionId);
                        }
                        flag = true;
                    }
                }
                Collection<String> dateCollection = multiMap.keys();
                for (String d : dateCollection) {
                    execService.execProcessInstance(
                        loginUser,
                        projectCode,
                        dependentProcessDefinitionId,
                        d,
                        commandType,
                        FailureStrategy.CONTINUE,
                        startNodeList,
                        taskDependType,
                        warningType,
                        warningGroupId,
                        runMode,
                        processInstancePriority,
                        workerGroup,
                        -1l,
                        timeout,
                        null,
                        null,
                        0
                        );
                }
            }
        }
    }

    @Override
    public ProcessStateVo selectCounts(Integer processDefinitionId) {

        return taskMapper.selectCounts(processDefinitionId);
    }

    public int selectcommoncounts(Integer processDefinitionId) {

        return taskMapper.selectcommoncounts(processDefinitionId);
    }
}
