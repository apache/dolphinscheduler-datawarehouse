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

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import org.apache.dolphinscheduler.common.enums.IOType;

import java.util.Date;

/**
 * process definition
 */
@Data
@TableName("t_ds_process_definition_io")
public class ProcessDefinitionIO {

    /**
     * id
     */
    @TableId(value = "id", type = IdType.AUTO)
    private int id;

    /**
     * name
     */
    private Long processDefinitionCode;

    /**
     * version
     */
    private long processDefinitionVersion;

    /**
     * release state :
     * @see IOType
     */
    private IOType ioType;

    /**
     * project id
     */
    private String ioName;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date createTime;

    /**
     * 任务id
     */
    private String taskId;

    /**
     * 任务名称
     */
    private String taskName;

    /**
     * 工作流名称
     */
    private String processDefinitionName;

    public ProcessDefinitionIO(int id, Long processDefinitionId, long processDefinitionVersion, IOType ioType, String ioName, Date createTime) {
        this.id = id;
        this.processDefinitionCode = processDefinitionId;
        this.processDefinitionVersion = processDefinitionVersion;
        this.ioType = ioType;
        this.ioName = ioName;
        this.createTime = createTime;
    }
    public ProcessDefinitionIO(Long processDefinitionId, long processDefinitionVersion, IOType ioType, String ioName) {
        this.processDefinitionCode = processDefinitionId;
        this.processDefinitionVersion = processDefinitionVersion;
        this.ioType = ioType;
        this.ioName = ioName;
        this.createTime = createTime;
    }

    public ProcessDefinitionIO(Long processDefinitionId, long processDefinitionVersion, IOType ioType, String ioName, String processDefinitionName, String taskId, String taskName) {
        this.processDefinitionCode = processDefinitionId;
        this.processDefinitionVersion = processDefinitionVersion;
        this.ioType = ioType;
        this.ioName = ioName;
        this.processDefinitionName = processDefinitionName;
        this.taskId = taskId;
        this.taskName = taskName;
    }

    public ProcessDefinitionIO(int id, Long processDefinitionId, long processDefinitionVersion, int ioType, String ioName, Date createTime, String taskId, String taskName, String processDefinitionName) {
        this.id = id;
        this.processDefinitionCode = processDefinitionId;
        this.processDefinitionVersion = processDefinitionVersion;
        this.ioType = IOType.getEnum(ioType);
        this.ioName = ioName;
        this.createTime = createTime;
        this.taskId = taskId;
        this.taskName = taskName;
        this.processDefinitionName = processDefinitionName;
    }
}
