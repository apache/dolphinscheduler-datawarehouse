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

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.util.Objects;

@Data
@TableName("bz_zr_process_type")
public class ProcessCreateType {

    /**
     * 该表的主键，唯一标识
     */
    private int id;

    /**
     * 工作流ID
     */
    private  Long process_id;

    /**
     * 标识创建该工作流的方式：0为非可视化创建，1为可视化创建
     */
    @TableId(value = "process_type")
    private int processCreateType;

    /**
     * 可视化创建需要的数据结构，非可视化方式为null
     */
    private String process_data_structure;

    public ProcessCreateType(Long process_id, int processCreateType, String process_data_structure) {
        this.process_id = process_id;
        this.processCreateType = processCreateType;
        this.process_data_structure = process_data_structure;
    }

    public ProcessCreateType() {
    }

    public void setProcess_data_structure(String process_data_structure) {
        this.process_data_structure = process_data_structure;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProcessCreateType that = (ProcessCreateType) o;
        return id == that.id && process_id == that.process_id && processCreateType == that.processCreateType && Objects.equals(process_data_structure, that.process_data_structure);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, process_id, processCreateType, process_data_structure);
    }

}
