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

package com.datawarehouse.be.pojo.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProcessInstanceDependencyVO {

    /**
     * 当前任务ID
     * */
    private Integer curTaskInstanceId;

    /**
     * 依赖的工作流ID
     * */
    private Long dependentProcessId;

    /**
     * 依赖的工作流实例ID
     * */
    private Integer dependentProcessInstanceId;

    /**
     * 依赖的工作流名称
     * */
    private String dependentProcessName;

    /**
     * 依赖的工作流实例名称
     * */
    private String dependentProcessInstanceName;

    /**
     * 依赖表名
     * */
    private String tableName;

    /**
     * 依赖任务开始执行时间
     * */
    private String startTime;

    /**
     * 依赖任务执行结束时间
     * */
    private String endTime;

    /**
     * 执行用户
     * */
    private String executorName;

    /**
     * 执行结果
     * */
    private String status;

    /**
     * 依赖的探分区任务实例 id
     * @return
     */
    private Integer dependentTaskInstanceId;

    /**
     * 依赖任务类型
     * @return
     */
    private String dependentTaskType;

    public void setStartTime(Date startTime) {
        if(startTime == null){
            this.startTime = "-";
        }else{
            Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.startTime = format.format(startTime);
        }
    }

    public void setEndTime(Date endTime) {
        if(endTime == null){
            this.endTime = "-";
        }else{
            Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.endTime = format.format(endTime);
        }
    }
}
