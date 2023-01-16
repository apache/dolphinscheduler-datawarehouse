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

import com.datawarehouse.be.pojo.params.AirflowDAGAddParam;
import com.datawarehouse.be.service.DagConverterService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.dolphinscheduler.api.controller.BaseController;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.utils.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import static org.apache.dolphinscheduler.api.enums.Status.CREATE_PROCESS_DEFINITION_ERROR;

@CrossOrigin
@Api(tags = "DAG_CONVERTER_TAG")
@RestController
@RequestMapping("datawarehouse/dagConverter")
public class DagConverterController extends BaseController {

    @Autowired
    private DagConverterService dagConverterService;

    @ApiOperation(value = "dagConverter", notes = "DAG_CONVERTER")
    @PostMapping(value = "/dagConverter")
    @ResponseStatus(HttpStatus.CREATED)
    @ApiException(CREATE_PROCESS_DEFINITION_ERROR)
    public Result dagConverter(
            @RequestBody AirflowDAGAddParam airflowDAGAddParam
    ) throws Exception {
        return returnDataList(dagConverterService.dagConverter(airflowDAGAddParam));
    }

}
