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

import com.datawarehouse.be.service.HiveMetaService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.dolphinscheduler.api.controller.BaseController;
import org.apache.dolphinscheduler.api.utils.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Api(value = "Get HiveMeta Info", tags = "HIVE META")
@RestController
@RequestMapping("/hivemeta")
@CrossOrigin()
public class HiveMetaController extends BaseController {
    private final static Logger logger = LoggerFactory.getLogger(HiveMetaController.class);
    @Autowired
    @Qualifier("hiveMetaService")
    HiveMetaService hiveMetaService;

    @GetMapping("/dbs")
    @ApiOperation(value = "获取库信息", httpMethod = "GET", produces = "application/json")
    public Result getHiveDbs() {
        Map<String, Object> result = hiveMetaService.getHiveDbs();
        logger.info("调用查询hive库信息接口");
        return returnDataList(result);
    }

    @GetMapping("/dbtabs")
    @ApiOperation(value = "获取库表信息", httpMethod = "GET", produces = "application/json")
    public Result getHiveTabsByDb(@RequestParam(value = "dbName", required = true) String dbName) {
        Map<String, Object> result = hiveMetaService.getHiveTabsByDb(dbName);
        return returnDataList(result);
    }

    @GetMapping("/keytabs")
    @ApiOperation(value = "根据模糊查询获取表信息", httpMethod = "GET", produces = "application/json")
    public Result getHiveTabsByKeyword(@RequestParam(value = "keyword", required = true) String keyword) {
        Map<String, Object> result = hiveMetaService.getHiveTabsByKeyword(keyword);
        return returnDataList(result);
    }

    @GetMapping("/tabinfo")
    @ApiOperation(value = "根据传入库名表名获取最后更新时间、生存周期")
    public Result getHiveTabInfoByDbTab(@RequestParam(value = "dbName") String dbName, @RequestParam(value = "tabName") String tabName) {
        Map<String, Object> result = hiveMetaService.getHiveTabInfo(dbName, tabName);
        return returnDataList(result);
    }

    @GetMapping("/partition")
    @ApiOperation(value = "获取库表分区字段及其说明", httpMethod = "GET", produces = "application/json")
    public Result getPartitionName(@RequestParam("dbName")String dbName, @RequestParam("tabName") String tabName){
        Map<String, Object> result = hiveMetaService.getPartitionName(dbName, tabName);
        return returnDataList(result);
    }

    @GetMapping("/columnlist")
    @ApiOperation(value = "获取表的字段", httpMethod = "GET", produces = "application/json")
    public Result getTableColumnList(@RequestParam("dbName")String dbName, @RequestParam("tableName") String tableName){
        Map<String, Object> result = hiveMetaService.getTableColumnList(dbName, tableName);
        return returnDataList(result);
    }

    @GetMapping("/isexists")
    @ApiOperation(value = "查询表是否存在", httpMethod = "GET", produces = "application/json")
    public Result isTableExists(@RequestParam("dbName")String dbName, @RequestParam("tableName") String tableName){
        Map<String, Object> result = hiveMetaService.isTableExists(dbName, tableName);
        return returnDataList(result);
    }
}
