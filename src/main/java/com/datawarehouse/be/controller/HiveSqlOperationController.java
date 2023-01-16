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

import com.datawarehouse.be.pojo.dto.QueryDto;
import com.datawarehouse.be.pojo.params.SqlParsingParam;
import com.datawarehouse.be.service.HiveSqlOperationService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.dolphinscheduler.api.controller.BaseController;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.util.Map;

@Api(value ="Hive SQL Operation", tags = "HIVE SQL OPERATION")
@RestController
@RequestMapping("/hive")
@CrossOrigin()
public class HiveSqlOperationController extends BaseController {

    @Autowired
    @Qualifier("hiveSqlOperationService")
    HiveSqlOperationService hiveSqlOperationService;

    @PostMapping("/parse/sql")
    @ApiOperation(value = "根据传入 SQL 执行查询获取示例数据", httpMethod = "POST")
    public Result getDemoDataBySQL(@RequestBody SqlParsingParam sqlParsingParam, @ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User user) throws Exception {
        Map<String,Object> result =  hiveSqlOperationService.getDemoDataBySQL(sqlParsingParam,user);
        return returnDataList(result);
    }

    @PostMapping("/visual/sql")
    @ApiOperation(value = "根据传入条件生成任务调度 sql", httpMethod = "POST")
    public Result getSqlByQueryCondition(@RequestBody QueryDto queryDto, @ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User user){
        Map<String,Object> result = hiveSqlOperationService.parseSql(queryDto, user);
        return returnDataList(result);
    }

}
