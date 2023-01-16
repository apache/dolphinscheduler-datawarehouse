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

import com.datawarehouse.be.exceptions.ExportDtoException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang.StringUtils;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class ExportDto {

    String dbName;
    String tableName;
    String dataSourceName;
    /**
     * 分区天数
     */
    Integer partitionDays;

    public void verify() throws Exception {
        if (StringUtils.isBlank(this.dataSourceName)){
            throw new ExportDtoException("数据源不能为空");
        }
        if (StringUtils.isBlank(this.dbName)){
            throw  new ExportDtoException("库名不能为空");
        }
        if (StringUtils.isBlank(this.tableName)){
            throw  new ExportDtoException("表名不能为空");
        }
        if (partitionDays == null){
            throw new ExportDtoException("导出的分区天数不能为空，请填写后再试~");
        }

        if (partitionDays<=0){
            throw  new ExportDtoException("导出的分区天数不能小于1，请修改后再试~");
        }
    }
}
