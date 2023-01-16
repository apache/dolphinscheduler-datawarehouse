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

@Data
@TableName("bz_zr_process_out_put")
public class OutPutEntity {
    /**
     * id
     */
    @TableId(value="id", type= IdType.AUTO)
    private int id;
    /**
     * 工作流Id
     */
    private Long processId;
    /**
     * 库名
     */
    private String dbName;
    /**
     * 表名
     */
    private String tableName;
    /**
     * 表类型
     */
    private String tableType;
    /**
     * 数据时效（天数）
     */
    private Integer validDays;


    public OutPutEntity(int id, Long processId, String dbName, String tableName, String tableType, Integer validDays) {
        this.id = id;
        this.processId = processId;
        this.dbName = dbName;
        this.tableName = tableName;
        this.tableType = tableType;
        this.validDays = validDays;
    }

    public OutPutEntity() {
    }

    public OutPutEntity(Long processId, String dbName, String tableName, String tableType, Integer validDays) {
        this.processId = processId;
        this.dbName = dbName;
        this.tableName = tableName;
        this.tableType = tableType;
        this.validDays = validDays;
    }

}
