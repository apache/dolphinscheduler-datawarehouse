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
import lombok.Data;

import java.util.UUID;

@Data
@TableName("bz_zr_process_out_put_export")
public class OutPutExportEntity {
    /**
     * id
     */
    @TableId(value = "id", type = IdType.AUTO)
    private int id;
    /**
     * 工作流Id
     */
    private Long processId;
    /**
     * 编号
     */
    private String code;
    /**
     * 数据源连接json
     */
    private String datasourceJson;
    /**
     * 工作流名称
     */
    private String datasourceName;

    public OutPutExportEntity() {
    }

    public OutPutExportEntity(int id, Long processId, String code, String datasourceName, String datasourceJson) {
        this.id = id;
        this.processId = processId;
        this.code = code;
        this.datasourceName = datasourceName;
        this.datasourceJson = datasourceJson;
    }

    public OutPutExportEntity(Long processId, String datasourceName, String datasourceJson) {
        this.processId = processId;
        this.code = UUID.randomUUID().toString();
        this.datasourceName = datasourceName;
        this.datasourceJson = datasourceJson;
    }
}
