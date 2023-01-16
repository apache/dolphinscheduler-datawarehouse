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

import com.datawarehouse.be.service.LineageService;
import org.apache.dolphinscheduler.api.controller.BaseController;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.task.sql.SqlParameters;
import org.apache.dolphinscheduler.dao.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@CrossOrigin
@RestController
@RequestMapping("tableLineage")
public class LineageController extends BaseController {

    @Autowired
    private LineageService lineageService;

    @PostMapping(value = "/querySqlLineage")
    @ResponseStatus(HttpStatus.OK)
    public Result querySqlLineage(User loginUser, @RequestBody Map<String, List<SqlParameters>> sqlParametersMap) {
        List<SqlParameters> sqlParameters = sqlParametersMap.get("sqlParameters");
        Map<String, Object> stringObjectMap = lineageService.querySqlLineage(sqlParameters);
        return returnDataList(stringObjectMap);
    }

}
