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

package com.datawarehouse.be.controller;

import com.datawarehouse.be.pojo.vo.ProcessStateVo;
import com.datawarehouse.be.service.CustomWarningService;
import com.datawarehouse.be.service.VisualProcessService;
import com.datawarehouse.be.service.impl.TaskServiceImpl;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.dolphinscheduler.api.enums.ExecuteType;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.service.ExecutorService;
import org.apache.dolphinscheduler.api.service.ProcessDefinitionService;
import org.apache.dolphinscheduler.api.service.WorkerGroupService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.CustomWarningTypeEnum;
import org.apache.dolphinscheduler.common.enums.FailureStrategy;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.common.enums.RunMode;
import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.common.utils.ParameterUtils;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.PhoneWarningMapper;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import springfox.documentation.annotations.ApiIgnore;

import java.text.ParseException;
import java.util.Map;

import static org.apache.dolphinscheduler.api.enums.Status.*;

@CrossOrigin
@Api(tags = "TASK_TAG")
@RestController
@RequestMapping("/projects/datawarehouse-task")
public class TaskController extends BaseController {

    private static final Logger logger = LoggerFactory.getLogger(TaskController.class);

    @Autowired
    TaskServiceImpl ziroomTaskService;

    @Autowired
    private ProcessDefinitionService processDefinitionService;

    @Autowired
    private ExecutorService execService;

    @Autowired
    WorkerGroupService workerGroupService;

    @Autowired
    CustomWarningService customWarningService;

    @Autowired
    VisualProcessService visualProcessService;

    @Autowired
    PhoneWarningMapper phoneWarningMapper;

    @Autowired
    ProjectMapper projectMapper;

    @ApiOperation(value = "queryTaskListPaging", notes = "ZIROOM_QUERY_TASK_INSTANCE_LIST_PAGING_NOTES")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "createUserName", value = "ZIROOM_CREATE_PROCESS_USERID", dataType = "String"),
        @ApiImplicitParam(name = "searchVal", value = "ZIROOM_SEARCH_VAL", type = "String"),
        @ApiImplicitParam(name = "pageNo", value = "ZIROOM_PAGE_NO", dataType = "Int", example = "1"),
        @ApiImplicitParam(name = "pageSize", value = "ZIROOM_PAGE_SIZE", dataType = "Int", example = "20")
    })
    @GetMapping("/list-paging")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_TASK_LIST_PAGING_ERROR)
    public Result queryTaskListPaging(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                      @RequestParam(value = "createUserName", required = false) String createUserName,
                                      @RequestParam(value = "searchVal", required = false) String searchVal,
                                      @RequestParam("pageNo") Integer pageNo,
                                      @RequestParam("pageSize") Integer pageSize) {

        logger.info("query datawarehouse task instance list, createUserName :{}, searchVal:{}",
                createUserName, searchVal);
        searchVal = ParameterUtils.handleEscapes(searchVal);
        Map<String, Object> result = ziroomTaskService.queryTaskListPaging(
                loginUser, createUserName, searchVal, pageNo, pageSize);
        return returnDataListPaging(result);
    }

    @ApiOperation(value = "queryTodaySchedulerStatisticsInfo", notes = "ZIROOM_QUERY_TODAY_SCHEDULER_STATISTICS_INFO")
    @GetMapping("/today-scheduler-statistics")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_TASK_LIST_PAGING_ERROR)
    public Result queryTodaySchedulerStatisticsInfo(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser) {

        logger.info("query datawarehouse today scheduler statistics, User :{}", loginUser);

        Map<String, Object> result = ziroomTaskService.queryTodaySchedulerInfo(loginUser);

        return returnDataList(result);
    }

    @ApiOperation(value = "releaseProcessDefinition", notes = "RELEASE_PROCESS_DEFINITION_NOTES")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "projectName", value = "PROJECT_NAME", required = true, dataType = "String"),
        @ApiImplicitParam(name = "processId", value = "PROCESS_DEFINITION_ID", required = true, dataType = "Long", example = "100"),
        @ApiImplicitParam(name = "releaseState", value = "PROCESS_DEFINITION_CONNECTS", required = true, dataType = "Int", example = "100"),
    })
    @PostMapping(value = "/release")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(RELEASE_PROCESS_DEFINITION_ERROR)
    public Result releaseProcessDefinition(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                           @RequestParam(value = "projectName",required = true) String projectName,
                                           @RequestParam(value = "processId", required = true) Long processId,
                                           @RequestParam(value = "releaseState", required = true) int stateInt) {

        logger.info("login user {}, release process definition, project name: {}, release state: {}",
                loginUser.getUserName(), projectName, stateInt);
        Project project = projectMapper.queryByName(projectName);
        Long projectCode = project.getCode();
        Long processCode = processId;
        ReleaseState state = ReleaseState.getEnum(stateInt);
        Map<String, Object> result = processDefinitionService.releaseProcessDefinition(
                loginUser,
                projectCode,
                processCode,
                state);
        return returnDataList(result);
    }

    @ApiOperation(value = "startProcessInstance", notes = "RUN_PROCESS_INSTANCE_NOTES")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "projectName", value = "PROJECT_NAME", required = true, dataType = "String"),
        @ApiImplicitParam(name = "processDefinitionId", value = "PROCESS_DEFINITION_ID", required = true, dataType = "Int", example = "1"),
        @ApiImplicitParam(name = "execType", value = "COMMAND_TYPE", dataType = "CommandType",required = true, example = "START_PROCESS,COMPLEMENT_DATA"),
        @ApiImplicitParam(name = "scheduleTime", value = "SCHEDULE_TIME", required = false, dataType = "String")
    })
    @PostMapping(value = "start-process-instance")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(START_PROCESS_INSTANCE_ERROR)
    public Result startProcessInstance(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                       @RequestParam(value = "projectName",required = true) String projectName,
                                       @RequestParam(value = "processDefinitionId",required = true) Long processDefinitionId,
                                       @RequestParam(value = "execType", required = true) CommandType execType,
                                       @RequestParam(value = "scheduleTime", required = false) String scheduleTime) throws ParseException {
        FailureStrategy failureStrategy = FailureStrategy.END;
        WarningType warningType = WarningType.NONE;
        TaskDependType taskDependType = TaskDependType.TASK_POST;
        RunMode runMode = RunMode.RUN_MODE_SERIAL;
        Priority processInstancePriority = Priority.MEDIUM;
        int warningGroupId = 0;
        String workerGroup = "default";
        String startNodeList = "";
        String receivers = "";
        String receiversCc = "";
        Integer timeout =  Constants.MAX_TASK_TIMEOUT;
        logger.info("login user {}, start process instance, project name: {}, process definition id: {}, schedule time: {}, "
                        + "failure policy: {}, node name: {}, node dep: {}, notify type: {}, "
                        + "notify group id: {},receivers:{},receiversCc:{}, run mode: {},process instance priority:{}, workerGroup: {}, timeout: {}",
                loginUser.getUserName(), projectName, processDefinitionId, scheduleTime,
                failureStrategy, startNodeList, taskDependType, warningType, workerGroup, receivers, receiversCc, runMode, processInstancePriority,
                workerGroup, timeout);
        Project project = projectMapper.queryByName(projectName);
        Integer expectedParallelismNumber = 1;
        int dryRun = 0;
        Map<String, String> startParamMap = null;
        Map<String, Object> checkResult = execService.startCheckByProcessDefinedCode(processDefinitionId);
        if(checkResult != null && checkResult.containsKey(Constants.STATUS) && !checkResult.get(Constants.STATUS).equals(Status.SUCCESS)) {
            return returnDataList(checkResult);
        }
        if (timeout == null) {
            timeout = Constants.MAX_TASK_TIMEOUT;
        }
        if (scheduleTime != null && execType.getCode() == CommandType.COMPLEMENT_DATA.getCode()) {
            ziroomTaskService.execDependentTaskWhenComplement(
                loginUser,
                project.getCode(),
                processDefinitionId,
                scheduleTime,
                execType,
                failureStrategy,
                startNodeList,
                taskDependType,
                warningType,
                warningGroupId,
                receivers,
                receiversCc,
                runMode,
                processInstancePriority,
                workerGroup,
                timeout
            );
        }
        Map<String, Object> result = execService.execProcessInstance(
                loginUser,
                project.getCode(),
                processDefinitionId,
                scheduleTime,
                execType,
                failureStrategy,
                startNodeList,
                taskDependType,
                warningType,
                warningGroupId,
                runMode,
                processInstancePriority,
                workerGroup,
                -1L,
                timeout,
                startParamMap,
                expectedParallelismNumber,
                dryRun);
        return returnDataList(result);
    }

    @ApiOperation(value = "deleteProcessDefinitionById", notes = "DELETE_PROCESS_DEFINITION_BY_ID_NOTES")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "projectName", value = "PROJECT_NAME", required = true, dataType = "String"),
        @ApiImplicitParam(name = "processDefinitionId", value = "PROCESS_DEFINITION_ID", required = true, dataType = "Int")
    })
    @GetMapping(value = "/delete")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(DELETE_PROCESS_DEFINE_BY_CODE_ERROR)
    public Result deleteProcessDefinitionById(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                              @RequestParam(value = "projectName",required = true) String projectName,
                                              @RequestParam("processDefinitionId") Long processDefinitionId
    ) {
        logger.info("delete process definition by id, login user:{}, project name:{}, process definition id:{}",
                loginUser.getUserName(), projectName, processDefinitionId);
        Project project = projectMapper.queryByName(projectName);
        Map<String, Object> result = processDefinitionService.deleteProcessDefinitionByCode(
                loginUser,
                project.getCode(),
                processDefinitionId);
        if (result.get("code") != null && (Integer)result.get("code") == 0) {
            customWarningService.deleteCustomWarning(processDefinitionId);
            visualProcessService.deleteVisualQueryDto(processDefinitionId);
        }

        return returnDataList(result);
    }

    @ApiOperation(value = "execute", notes = "EXECUTE_ACTION_TO_PROCESS_INSTANCE_NOTES")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "projectName", value = "PROJECT_NAME", required = true, dataType = "String"),
        @ApiImplicitParam(name = "processInstanceId", value = "PROCESS_INSTANCE_ID", required = true, dataType = "Int", example = "100"),
        @ApiImplicitParam(name = "executeType", value = "EXECUTE_TYPE", required = true, dataType = "ExecuteType")
    })
    @PostMapping(value = "instance/execute")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(EXECUTE_PROCESS_INSTANCE_ERROR)
    public Result execute(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                          @RequestParam(value = "projectName",required = true) String projectName,
                          @RequestParam("processInstanceId") Integer processInstanceId,
                          @RequestParam("executeType") ExecuteType executeType //REPEAT_RUNNING\STOP
    ) {

        logger.info("execute command, login user: {}, project:{}, process instance id:{}, execute type:{}",
                loginUser.getUserName(), projectName, processInstanceId, executeType);
        Project project = projectMapper.queryByName(projectName);
        Map<String, Object> result = execService.execute(
                loginUser,
                project.getCode(),
                processInstanceId,
                executeType);
        if (executeType.equals(ExecuteType.REPEAT_RUNNING)) {
            ziroomTaskService.execHivePartitionSensorAfterUpdate();
        }
        return returnDataList(result);
    }

    @ApiOperation(value = "queryProcessInstanceList", notes = "QUERY_PROCESS_INSTANCE_LIST_NOTES")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "projectName", value = "PROJECT_NAME", required = true, dataType = "String"),
        @ApiImplicitParam(name = "processDefinitionId", value = "PROCESS_DEFINITION_ID", dataType = "Int", example = "0"),
        @ApiImplicitParam(name = "startDate",value = "start_date",type = "String"),
        @ApiImplicitParam(name = "endDate",value = "end_date",type = "String"),
        @ApiImplicitParam(name = "executorName", value = "EXECUTOR_NAME", type = "String"),
        @ApiImplicitParam(name = "pageNo", value = "PAGE_NO", dataType = "Int", example = "1"),
        @ApiImplicitParam(name = "pageSize", value = "PAGE_SIZE", dataType = "Int", example = "10")
    })
    @GetMapping(value = "instance/list-paging")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_PROCESS_INSTANCE_LIST_PAGING_ERROR)
    public Result queryProcessInstanceList(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                           @RequestParam(value = "projectName",required = true) String projectName,
                                           @RequestParam(value = "processDefinitionId", required = false, defaultValue = "0") Long processDefinitionId,
                                           @RequestParam(value = "startDate", required = false) String startTime,
                                           @RequestParam(value = "endDate", required = false) String endTime,
                                           @RequestParam(value = "stateVal",required = false) String stateVal,
                                           @RequestParam(value = "executorName", required = false) String executorName,
                                           @RequestParam("pageNo") Integer pageNo,
                                           @RequestParam("pageSize") Integer pageSize) {

        logger.info("query all process instance list, login user:{},project name:{}, define id:{},"
                +
                "start time:{}, end time:{},executor name:{},page number:{}, page size:{}",
            loginUser.getUserName(), projectName, processDefinitionId, startTime, endTime, executorName, pageNo, pageSize);
        Map<String, Object> result = ziroomTaskService.queryProcessInstanceList(
            loginUser, projectName, processDefinitionId, startTime,endTime,stateVal,executorName,  pageNo, pageSize);
        return returnDataListPaging(result);
    }

    @GetMapping(value = "instance/counts")
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(value = "ProcessInstanceListCount", notes = "COUNT_PROCESS_INSTANCE_LIST_NOTES")
    @ApiException(value = COUNT_PROCESS_INSTANCE_LIST_PAGING_ERROR)
    @ApiImplicitParams({
        @ApiImplicitParam(name = "processDefinitionId", value = "PROCESS_DEFINITION_ID", dataType = "Int"),
    })
    public Result ProcessInstanceListCount(
        @RequestParam(value = "processDefinitionId", required = true) Integer processDefinitionId
    ) {
        ProcessStateVo processStateVo = ziroomTaskService.selectCounts(processDefinitionId);
        int common = ziroomTaskService.selectcommoncounts(processDefinitionId);

        if(processStateVo == null && common == 0) {
            return Result.success("1");
        }else if(processStateVo == null && common > 0) {
            return Result.success("2");
        } else if(processStateVo != null && common == 0) {
            if("0,1,8,10,11".contains(processStateVo.getState().toString())){
                return Result.success("3");
            }else{
                return Result.success("1");
            }
        }else {
            return Result.success("2");
        }
    }

    @ApiOperation(value = "complementProcessInstanceList", notes = "COMPLEMENT_PROCESS_INSTANCE_LIST_NOTES")
    @ApiImplicitParams({
        @ApiImplicitParam(name = "projectName", value = "PROJECT_NAME", required = true, dataType = "String"),
        @ApiImplicitParam(name = "processDefinitionId", value = "PROCESS_DEFINITION_ID", dataType = "Int", example = "0"),
        @ApiImplicitParam(name = "startDate",value = "start_date",type = "String") ,
        @ApiImplicitParam(name = "endDate",value = "end_date",type = "String") ,
        @ApiImplicitParam(name = "stateVal",value = "state_val",type = "String"),
        @ApiImplicitParam(name = "executorName", value = "EXECUTOR_NAME", type = "String"),
        @ApiImplicitParam(name = "startorder",value = "start_order",type = "Int"),
        @ApiImplicitParam(name = "endorder",value = "end_order",type = "Int"),
        @ApiImplicitParam(name = "runorder",value = "run_order",type = "Int"),
        @ApiImplicitParam(name = "taskorder",value = "task_order",type = "Int"),
        @ApiImplicitParam(name = "pageNo", value = "PAGE_NO", dataType = "Int", example = "1"),
        @ApiImplicitParam(name = "pageSize", value = "PAGE_SIZE", dataType = "Int", example = "10")
    })

    @GetMapping(value = "complement/list-paging")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(COMPLEMENT_PROCESS_INSTANCE_LIST_PAGING_ERROR)
    public Result complementProcessInstanceList(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                @RequestParam(value = "projectName",required = true) String projectName,
                                                @RequestParam(value = "processDefinitionId", required = false, defaultValue = "0") long processDefinitionId,
                                                @RequestParam(value = "startDate", required = false) String startTime,
                                                @RequestParam(value = "endDate", required = false) String endTime,
                                                @RequestParam(value = "stateVal",required = false) String stateVal,
                                                @RequestParam(value ="startorder",required = false) Integer  startorder,
                                                @RequestParam(value ="endorder",required = false) Integer  endorder,
                                                @RequestParam(value ="runorder",required = false) Integer  runorder,
                                                @RequestParam(value ="taskorder",required = false,defaultValue = "0") Integer  taskorder,
                                                @RequestParam(value = "executorName", required = false) String executorName, @RequestParam("pageNo") Integer pageNo,
                                                @RequestParam("pageSize") Integer pageSize) {


        logger.info("query all process instance list, login user:{},project name:{}, define id:{}," +
                "start time:{}, end time:{},executor name:{},page number:{}, page size:{}",
            loginUser.getUserName(), projectName, processDefinitionId,startTime,endTime ,executorName, pageNo, pageSize);
        Map<String, Object> result = ziroomTaskService.complementProcessInstanceList(
            loginUser, projectName, processDefinitionId, startTime,endTime,stateVal,executorName,
            startorder,endorder,runorder,taskorder,
            pageNo, pageSize);

        return returnDataListPaging(result);
    }

    @GetMapping(value = "/alarm/state")
    @ResponseStatus(HttpStatus.OK)
    @ApiOperation(value = "报警状态")
    public Result Alarmstate(@RequestParam(value = "processDefinitionId", required = true) Integer processDefinitionId) {
        int phonetotle = phoneWarningMapper.selectphonestate(processDefinitionId, CustomWarningTypeEnum.PHONE.getType());
        int wechartotle = phoneWarningMapper.selectphonestate(processDefinitionId, CustomWarningTypeEnum.ENWECHAT.getType());
        if (phonetotle > 0) {
            return  Result.success(true,"电话告警");
        } else if (wechartotle > 0 ) {
            return  Result.success(true,"微信告警");
        } else{
            return  Result.success(false,"用户没有定义告警");
        }
    }
}
