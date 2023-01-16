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

package com.datawarehouse.be.pojo.dos;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Api(value = "AirflowDagAdd", tags = "Airflow中DAG对象（新增）")
public class ProcessAddDO {

    private Long projectCode;

    private String name;

    @ApiModelProperty(name = "description", value = "工作流描述", dataType = "java.lang.String", example = "test", required = true)
    private String description;

    private String creator;

    @ApiModelProperty(name = "processJsonDO", value = "工作流Json对象", dataType = "ProcessJsonDO", example = "", required = true)
    private ProcessJsonDO processJsonDO;

    public void wrap(Long projectCode, String name, String description, String creator) {
        this.projectCode = projectCode;
        this.name = name;
        this.description = description;
        this.creator = creator;
    }
}
