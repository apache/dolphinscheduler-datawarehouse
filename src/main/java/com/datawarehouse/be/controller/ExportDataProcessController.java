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

import com.datawarehouse.be.pojo.dto.ExportDto;
import com.datawarehouse.be.service.ExportDataProcessService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.dolphinscheduler.api.controller.BaseController;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.entity.User;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.util.HashMap;
import java.util.Optional;

@CrossOrigin
@Api(tags = "EXPORT_DATA_PROCESS")
@RestController
@RequestMapping("/exportDataProcess")
public class ExportDataProcessController extends BaseController {
    private static final Logger logger = LoggerFactory.getLogger(ExportDataProcessController.class);

    @Autowired
    @Qualifier("exportDataProcessService")
    ExportDataProcessService exportDataProcessService;

    @PostMapping("/AddEditExportDataProcess")
    @ApiOperation(value = "增加/修改数据导出工作流", httpMethod = "POST")
    public Result addEditExportDataProcess(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                           @RequestBody ExportDto exportDto) throws Exception {
        logger.info("Create or edit an exprot data work process, login user:{}, exportDto:{}",
                        loginUser, Optional.ofNullable(exportDto).map(e -> JSON.toString(e)).orElse(null));
        exportDataProcessService.addEditExportDataProcess(loginUser, exportDto);
        return success();
    }

    @GetMapping("/isExistClickhouseTable")
    @ApiOperation(value = "检查clickhouse表是否存在", httpMethod = "GET")
    public Result isExistClickhouseTable(@RequestParam("dbName") String dbName, @RequestParam("tableName") String tableName){

        HashMap<String, Boolean> map = new HashMap<>();
        if(exportDataProcessService.isExistClickhouseTable(dbName, tableName)) {
            map.put("chTabIsExist", true);
        } else {
            map.put("chTabIsExist", false);
        }
        return success(map);
    }
}
