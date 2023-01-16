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

package com.datawarehouse.be.service;

import org.apache.dolphinscheduler.common.enums.IOType;
import com.datawarehouse.be.pojo.dos.ProcessDefinitionIODO;

import java.util.List;
import java.util.Map;

public interface ProcessDefinitionIOService {
    /**
     * 根据输出库表名，查询process信息
     * */
    Map<String,Object> queryByIO(IOType ioType, List<String> outputs);

    /**
     * 根据输出库表名，查询process信息
     * */
    List<ProcessDefinitionIODO> query(IOType ioType, List<String> tableList);

}
