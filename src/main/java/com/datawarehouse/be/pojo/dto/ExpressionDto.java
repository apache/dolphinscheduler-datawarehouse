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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ApiModel("表达式")
public class ExpressionDto {

    /**
     * 库名
     */
    @ApiModelProperty("库名")
    private String dbName;

    /**
     * 表名
     */
    @ApiModelProperty("表名")
    private String tableName;

    /**
     * 字段名
     */
    @ApiModelProperty("字段名")
    private String columnName;

    /**
     * 字段类型
     */
    @ApiModelProperty("字段类型")
    private String typeName;


    /**
     * 注释
     */
    @ApiModelProperty("注释")
    private String comment;

    /**
     * 表达式类型
     */
    @ApiModelProperty("表达式类型")
    private Integer expressionType;

    /**
     * 值
     */
    @ApiModelProperty("值")
    private String value;

    /**
     * 介于 开始值
     */
    @ApiModelProperty("介于开始值")
    private String start;

    /**
     * 介于结束值
     */
    @ApiModelProperty("介于结束值")
    private String end;

    /**
     * 是否使其不可删除
     */
    @ApiModelProperty("是否使其不可删除")
    private Boolean disabled;


    /**
     * 表达式索引
     */
    @ApiModelProperty("表达式索引")
    private Integer index;

}