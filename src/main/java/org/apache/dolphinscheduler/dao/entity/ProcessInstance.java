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

package org.apache.dolphinscheduler.dao.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;

import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProcessInstance {

    /**实例id**/
    private int id;
    /**流程实例名称**/
    private String name;
    /**流程定义id**/
    private Long processDefinitionCode;
    /**流程实例执行状态**/
    private ExecutionStatus state;
    /**流程实例开始时间**/
    private Date startDateTime;
    /**流程实例完成时间**/
    private Date endDateTime;
    /**流程实例开始时间**/
    private String startTime;
    /**流程实例完成时间**/
    private String endTime;
    /**流程实例执行时长**/
    private int runTimes;
    /**格式化后的流程实例执行时长**/
    private String duration;
    /**执行用户名**/
    private String userName;
    /**项目名**/
    private String projectName;
    /**状态名**/
    private String  stateName;

    private String scheduleDateTime;
}
