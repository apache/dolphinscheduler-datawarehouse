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
import lombok.Data;
import org.apache.dolphinscheduler.common.enums.DependentRelation;
import org.apache.dolphinscheduler.common.model.DependentTaskModel;

import java.io.Serializable;
import java.util.List;

/**
 * 依赖
 */
@Data
public class DependenceDTOV2 implements Serializable {

    @JsonProperty("relation")
    private DependentRelation relation = DependentRelation.AND;

    @JsonProperty("dependTaskList")
    private List<DependentTaskModel> dependTaskList;
}