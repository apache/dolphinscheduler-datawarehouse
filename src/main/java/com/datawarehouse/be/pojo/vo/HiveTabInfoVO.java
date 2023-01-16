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

import java.text.SimpleDateFormat;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HiveTabInfoVO {

    private static SimpleDateFormat simpleDateFormat =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    /**
     * 最后的更新时间
     */
    private String lastModifiedTime;

    /**
     * 生存周期
     */
    private Integer partitionRetentionPeriod;


    /**
     * 设置最后的修改时间，对传入时间戳进行转换
     * @param modifiedTime
     */
    public void setLastModifiedTime(String modifiedTime) {
        // 按秒计算
        if (modifiedTime.length() == 10) {
            modifiedTime += "000";
        }

        // 时间戳格式的转换
        Long stamp = new Long(modifiedTime);
        this.lastModifiedTime = simpleDateFormat.format(new Date(stamp));
    }
}
