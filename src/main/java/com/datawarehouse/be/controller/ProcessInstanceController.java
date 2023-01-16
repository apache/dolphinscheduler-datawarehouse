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

import com.datawarehouse.be.service.ProcessInstanceService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.dolphinscheduler.api.controller.BaseController;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.utils.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

import static org.apache.dolphinscheduler.api.enums.Status.QUERY_DETAIL_OF_PROCESS_DEFINITION_ERROR;
import static org.apache.dolphinscheduler.api.enums.Status.QUERY_PROCESS_INSTANCE_BY_ID_ERROR;

@CrossOrigin
@Api(tags = "PROCESS_INSTANCE")
@RestController
@RequestMapping("ProcessInstance")
public class ProcessInstanceController extends BaseController {

    @Autowired
    private ProcessInstanceService ziroomProcessInstanceService;

    @ApiOperation(value = "queryZiroomProcessInstanceById", notes = "查询任务实例详情")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "projectName", value = "项目名称", dataType = "String"),
            @ApiImplicitParam(name = "processInstanceId", value = "PROCESS_INSTANCE_ID", dataType = "Int", example = "100")
    })
    @GetMapping(value = "/queryZiroomProcessInstanceById")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_PROCESS_INSTANCE_BY_ID_ERROR)
    public Result queryZiroomProcessInstanceById(@RequestParam(name = "projectName") String projectName,
                                                 @RequestParam("processInstanceId") Integer processInstanceId
    ) {
        Map<String, Object> result = ziroomProcessInstanceService.queryCustomProcessInstanceById(projectName, processInstanceId);
        return returnDataList(result);
    }

    @ApiOperation(value = "queryDependencyByInstanceId", notes = "查询依赖列表")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "projectName", value = "项目名称", dataType = "String"),
            @ApiImplicitParam(name = "processInstanceId", value = "工作流实例ID", required = true, dataType = "Int", example = "100")
    })
    @GetMapping(value = "/queryDependencyByInstanceId")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_DETAIL_OF_PROCESS_DEFINITION_ERROR)
    public Result queryDependencyByInstanceId(@RequestParam(name = "projectName") String projectName,
                                              @RequestParam("processInstanceId") Integer processInstanceId
    ) {
        Map<String, Object> result = ziroomProcessInstanceService.queryDependencyByInstanceId(projectName, processInstanceId);
        return returnDataList(result);
    }

    @ApiOperation(value = "queryOutputByInstanceId", notes = "查询任务产出接口")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "projectName", value = "项目名称", dataType = "String"),
            @ApiImplicitParam(name = "processInstanceId", value = "工作流实例ID", required = true, dataType = "Int", example = "100")
    })
    @GetMapping(value = "/queryOutputByInstanceId")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_DETAIL_OF_PROCESS_DEFINITION_ERROR)
    public Result queryOutputByInstanceId(
                                              @RequestParam(name = "projectName") String projectName,
                                              @RequestParam("processInstanceId") Integer processInstanceId
    ) {
        Map<String, Object> result = ziroomProcessInstanceService.queryOutputByInstanceId(projectName, processInstanceId);
        return returnDataList(result);
    }
}
