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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.apache.dolphinscheduler.api.controller.BaseController;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.util.Map;

import static org.apache.dolphinscheduler.api.enums.Status.QUERY_DETAIL_OF_PROCESS_DEFINITION_ERROR;

@CrossOrigin
@Api(tags = "PROCESS_DEFINITION")
@RestController
@RequestMapping("ProcessDefintion")
public class ProcessDefinitionController extends BaseController {

    @Autowired
    private com.datawarehouse.be.service.ProcessDefinitionService ProcessDefinitionService;

    @ApiOperation(value = "queryZiroomProcessDefinitionById", notes = "查询工作流详情")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "projectName", value = "项目名称", dataType = "String"),
            @ApiImplicitParam(name = "processId", value = "工作流ID", required = true, dataType = "Int", example = "100")
    })
    @GetMapping(value = "/queryById")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(QUERY_DETAIL_OF_PROCESS_DEFINITION_ERROR)
    public Result queryZiroomProcessDefinitionById(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                   @RequestParam(name = "projectName") String projectName,
                                                   @RequestParam("processId") Long processId
    ) {
        Map<String, Object> result = ProcessDefinitionService.queryZiroomProcessDefinitionById(loginUser, projectName, processId);
        return returnDataList(result);
    }

}
