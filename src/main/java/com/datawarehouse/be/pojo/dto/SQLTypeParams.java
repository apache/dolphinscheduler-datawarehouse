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

package com.datawarehouse.be.pojo.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class SQLTypeParams implements Serializable {
    @ApiModelProperty(name = "type", value = "动作类型，默认HIVE", dataType = "java.lang.String", example = "SQL")
    @JsonProperty("type")
    private String type = "HIVE";

    @ApiModelProperty(name = "datasource", value = "数据源，默认值为1（Hive数据源），迁移业务线之前约定好值", dataType = "java.lang.Integer", required = true)
    @JsonProperty("datasource")
    private int datasource = 1;

    @ApiModelProperty(name = "sql", value = "查询语句", dataType = "java.lang.String", required = true)
    @JsonProperty("sql")
    private String sql;

    @ApiModelProperty(name = "sqlType", value = "SQL类型，默认为1，非查询，0为查询", dataType = "java.lang.String", example = "1")
    @JsonProperty("sqlType")
    private String sqlType = "1";

    @ApiModelProperty(name = "sendEmail", value = "发送邮件，默认为false", dataType = "java.lang.Boolean")
    @JsonProperty("sendEmail")
    private Boolean sendEmail = false;

    @ApiModelProperty(name = "displayRows", value = "显示行数，默认10行", dataType = "java.lang.Integer")
    @JsonProperty("displayRows")
    private Integer displayRows = 10;

    @ApiModelProperty(name = "udfs", value = "自定义函数,默认为空字符串", dataType = "java.lang.String")
    @JsonProperty("udfs")
    private String udfs = "";

    /**
     * show type
     * 0 TABLE
     * 1 TEXT
     * 2 attachment
     * 3 TABLE+attachment
     */
    private String showType = "0";

    @ApiModelProperty(name = "connParams", value = "连接参数", dataType = "java.lang.String")
    @JsonProperty("connParams")
    private String connParams = "";

    @ApiModelProperty(name = "preStatements", value = "前置SQL，由DS处理，解决多条SQL的问题", dataType = "java.util.List")
    @JsonProperty("preStatements")
    private List<String> preStatements = Lists.newArrayList();

    @ApiModelProperty(name = "postStatements", value = "后置SQL，由DS处理，解决多条SQL的问题", dataType = "java.util.List")
    @JsonProperty("postStatements")
    private List<String> postStatements = Lists.newArrayList();

    @ApiModelProperty(name = "groupId", value = "分组Id", dataType = "java.lang.Object", hidden = true)
    @JsonProperty("groupId")
    private Object groupId;

    @ApiModelProperty(name = "title", value = "标题，默认为空字符串", dataType = "java.lang.String")
    @JsonProperty("title")
    private String title = "";

    @ApiModelProperty(name = "localParams", value = "任务参数", dataType = "java.util.List")
    @JsonProperty("localParams")
    private List<String> localParams = Lists.newArrayList();

    @JsonProperty("limit")
    private int limit =100;

    @JsonProperty("conditionResult")
    private ConditionResultDTO conditionResult = new ConditionResultDTO();

    @JsonProperty("dependence")
    private DependenceDTO dependence;

    @JsonProperty("waitStartTimeout")
    private String waitStartTimeout;

    @JsonProperty("switchResult")
    private String switchResult;

    public void wrap(Integer datasource) {
        this.setDatasource(datasource);
    }


}