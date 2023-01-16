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
import org.apache.dolphinscheduler.common.enums.DependentRelation;

import java.io.Serializable;
import java.util.List;

@Data
public class DependTaskListDTO implements Serializable {

    @ApiModelProperty(name = "relation", value = "连接条件，默认AND", dataType = "java.lang.String")
    @JsonProperty("relation")
    private DependentRelation relation;

    @ApiModelProperty(name = "dependItemList", value = "条件项集合", dataType = "java.util.List", required = true)
    @JsonProperty("dependItemList")
    private List<DependItemListDTO> dependItemList;

    public void init(){
        this.setRelation(DependentRelation.AND);
        this.setDependItemList(Lists.newArrayList());
    }


}