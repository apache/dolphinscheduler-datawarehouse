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

import com.datawarehouse.be.service.StaffInfoService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.dolphinscheduler.api.controller.BaseController;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.util.Map;

@Api(value = "get staff info", tags = "STAFF INFO")
@RestController
@CrossOrigin()
public class StaffInfoController extends BaseController {

    @Autowired
    StaffInfoService staffInfoService;

    @PostMapping("/hive/staff")
    @ApiOperation(value = "获取员工信息，电话及邮箱信息", httpMethod = "POST", produces = "application/json")
    public Result getStaffInfoHive(@RequestParam(value = "keyword", required = false) String keyword) {
        Map<String, Object> result = staffInfoService.getStaffInfoByHive(keyword);
        return returnDataList(result);
    }

    @GetMapping("/emailpre")
    @ApiOperation(value = "获取员工信息，电话及邮箱信息", httpMethod = "GET", produces = "application/json")
    public Result getStaffInfoByEmailPre(@RequestParam(value = "keyword", required = false) String keyword) {
        Map<String, Object> result = staffInfoService.getStaffInfoByEmailPre(keyword);
        return returnDataList(result);
    }

    @GetMapping("/userdepdatabase")
    @ApiOperation(value = "获取用户所拥有权限的hive库", httpMethod = "GET")
    public Result getUserDepDatabase(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser) {
        Map<String, Object> result = staffInfoService.getUserDepDatabases(loginUser.getId());
        return returnDataList(result);
    }

}
