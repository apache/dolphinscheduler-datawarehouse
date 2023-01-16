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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.datawarehouse.be.pojo.params.AddEditProcessParam;
import com.datawarehouse.be.pojo.params.SqlParsingParam;
import com.datawarehouse.be.pojo.vo.ProcessDetailVO;
import com.datawarehouse.be.pojo.vo.SqlParsingVO;
import com.datawarehouse.be.service.ProcessService;
import com.datawarehouse.be.service.ProcessTikService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.dolphinscheduler.api.controller.BaseController;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.entity.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.util.Optional;

@CrossOrigin
@Api(tags = "PROCESS_TAG")
@RestController
@RequestMapping("datawarehouse/process")
public class ProcessController extends BaseController {

    private static final Logger logger = LoggerFactory.getLogger(ProcessController.class);

    @Autowired
    private ProcessService processService;

    @Autowired
    private ProcessTikService processTikService;

    @ApiOperation(value = "sqlParsing", notes = "SQL_PARSING")
    @PostMapping(value = "/sqlParsing")
    public Result sqlParsing(@RequestBody SqlParsingParam sqlParsingParam, @ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser) {
        logger.info("sqlParsingParam:{}, loginUser:{}", Optional.ofNullable(sqlParsingParam).map(JSON::toJSONString).orElse(null), Optional.ofNullable(loginUser).map(e -> JSON.toJSONString(e)).orElse(null));
        SqlParsingVO sqlParsingVO = processService.sqlParsing(sqlParsingParam, loginUser);
        return success("成功", sqlParsingVO != null ? JSON.toJSONString(sqlParsingVO, SerializerFeature.WriteMapNullValue) : null);
    }

    @ApiOperation(value = "addEditProcessParam", notes = "SQL_PARSING")
    @PostMapping(value = "/addEditProcessParam")
    public Result addEditProcessParam(@RequestBody AddEditProcessParam addEditProcessParam, @ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser) throws Exception {
        logger.info("addEditProcessParam:{}, loginUser:{}", Optional.ofNullable(addEditProcessParam).map(JSON::toJSONString).orElse(null), Optional.ofNullable(loginUser).map(e -> JSON.toJSONString(e)).orElse(null));
         processService.addEditProcess(addEditProcessParam, loginUser);
        return success();
    }

    @ApiOperation(value = "getProcessDetail", notes = "GET_PROCESS_DETAIL")
    @GetMapping(value = "/getProcessDetail")
    public Result getProcessDetail(Long processId, @ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser) {
        logger.info("processId:{}, loginUser:{}", processId, Optional.ofNullable(loginUser).map(JSON::toJSONString).orElse(null));
        ProcessDetailVO processDetailVO = processService.queryProcessDefinitionById(loginUser, processId);
        return success("成功", processDetailVO != null ? JSON.toJSONString(processDetailVO, SerializerFeature.WriteMapNullValue) : null);
    }

    @ApiOperation(value = "fixSensorProcessJson", notes = "FIX_SENSOR_PROCESS_JSON")
    @GetMapping(value = "/fixSensorProcessJson")
    public Result fixSensorProcessJson() {
        processTikService.fixSensorProcessJson();
        return success();
    }

    @ApiOperation(value = "changeSensorProcessPriority", notes = "FIX_SENSOR_PROCESS_JSON")
    @GetMapping(value = "/changeSensorProcessPriority")
    public Result changeSensorProcessPriority() {
        processTikService.changeSensorProcessPriority();
        return success();
    }
}
