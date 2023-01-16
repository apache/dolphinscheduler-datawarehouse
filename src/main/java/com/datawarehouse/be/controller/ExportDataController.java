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

import com.datawarehouse.be.service.ExportDataService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;
import springfox.documentation.annotations.ApiIgnore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@CrossOrigin
@Api(tags = "EXPORT_DATA_INFO")
@RestController
@RequestMapping("/exportDataInfo")
public class ExportDataController extends BaseController {
    private static final Logger logger = LoggerFactory.getLogger(ExportDataController.class);

    @Autowired
    @Qualifier("exportDataService")
    ExportDataService exportDataService;

    @GetMapping(value = "instance/list_paging")
    @ApiOperation(value = "获取该表的数据导出工作流实例", httpMethod = "GET")
    @ApiException(Status.QUERY_PROCESS_INSTANCE_LIST_PAGING_ERROR)
    public Result queryExportDataProcessInstanceList(@RequestParam(value = "dbName",required = true) String dbName,
                                                     @RequestParam(value = "tableName",required = true) String tableName,
                                                     @RequestParam(value  = "startTimeFilterVal", required = false) String startTimeFilterVal,
                                                     @RequestParam("pageNo") Integer pageNo,
                                                     @RequestParam("pageSize")Integer pageSize){
        logger.info("Query all process instance list,  database name:{}, table name:{}"
                        +" start time Filter Value:{}, page number:{}, page size:{}",
                        Optional.ofNullable(dbName).map(e -> JSON.toString(e)).orElse(null),
                        Optional.ofNullable(tableName).map(e -> JSON.toString(e)).orElse(null),
                        Optional.ofNullable(startTimeFilterVal).map(e -> JSON.toString(e)).orElse(null),
                        pageNo,pageSize);
        Map<String, Object> result = exportDataService.queryExportDataProcessInstanceList(
                dbName, tableName, startTimeFilterVal, pageNo, pageSize);
        if (result == null){
            Result<List<ProcessInstance>> objectResult = new Result<>();
            List<ProcessInstance> list = new ArrayList<>();
            objectResult.setData(list);
            objectResult.setMsg("成功");
            objectResult.setCode(0);
            return objectResult;
        }
        else {
            return returnDataListPaging(result);
        }
    }

    @GetMapping("/tableDetail")
    @ApiOperation(value = "获取该表的最后更新时间、分区天数", httpMethod = "GET")
    public Result getTableDetails(@RequestParam("dbName") String dbName, @RequestParam("tableName") String tableName, String dataSourceName){
        logger.info("Get the details, db name:{}, table name:{}, data source name:{}",
                Optional.ofNullable(dbName).map(e -> JSON.toString(e)).orElse(null),
                Optional.ofNullable(tableName).map(e -> JSON.toString(e)).orElse(null),
                Optional.ofNullable(dataSourceName).map(e -> JSON.toString(e)).orElse(null));
        Map<String, Object> result = exportDataService.getTableDetails(dbName, tableName, dataSourceName);
        return returnDataList(result);
    }

    @GetMapping("/getDTHiveTable")
    @ApiOperation(value = "获取该库下分区为DT的所有表", httpMethod = "GET")
    public Result getDTHiveTable(@RequestParam("dbName") String dbName){
        logger.info("Get DT Hive Table，db name:{}", Optional.ofNullable(dbName).map(e -> JSON.toString(e)).orElse(null));
        Map<String, Object> result = exportDataService.getDTHiveTable(dbName);
        return  returnDataList(result);
    }

    @GetMapping("/fuzzyQueryTabs")
    @ApiOperation(value = "模糊查询获取表信息", httpMethod = "GET")
    public Result getTabsByFuzzyQuery(@RequestParam(value = "keyword", required = true)String keyword){
        logger.info("Fuzzy query to obtain table information based on keyword:{}", keyword);
        Map<String, Object> result = exportDataService.getTabsByFuzzyQuery(keyword);
        return returnDataList(result);
    }

    @GetMapping("/VerifyPermission")
    @ApiOperation(value = "校验该用户是否为表的技术负责人", httpMethod = "GET")
    public Result verifyPermission(@ApiIgnore @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                   @RequestParam(value = "dbName") String dbName, @RequestParam(value = "tableName") String tableName){
        logger.info("Verify that the user is the owner of the db.table, login user:{}, db name:{}, table name:{}", loginUser
                ,Optional.ofNullable(dbName).map(e -> JSON.toString(e)).orElse(null), Optional.ofNullable(tableName).map(e -> JSON.toString(e)).orElse(null));
        Map<String, Object>  result = exportDataService.verifyPermission(loginUser, dbName, tableName);
        return returnDataList(result);
    }

}
