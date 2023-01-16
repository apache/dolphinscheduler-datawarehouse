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
package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.ibatis.annotations.Param;

import java.util.Date;

public interface ProcessInstanceMapper extends BaseMapper<ProcessInstance> {


    IPage<ProcessInstance> queryProcessInstanceListPaging(Page<ProcessInstance> page,
                                                          @Param("projectCode") int projectCode,
                                                          @Param("stateVal") String stateVal,
                                                          @Param("processDefinitionCode") long processDefinitionCode,
                                                          @Param("executorId") Integer executorId,
                                                          @Param("startTime") Date startTime,
                                                          @Param("endTime") Date endTime);


    IPage<ProcessInstance> complementProcessInstanceListPaging(Page<ProcessInstance> page,
                                                               @Param("projectCode") int projectCode,
                                                               @Param("stateVal") String stateVal,
                                                               @Param("processDefinitionCode") long processDefinitionCode,
                                                               @Param("executorId") Integer executorId,
                                                               @Param("startTime") Date startTime,
                                                               @Param("endTime") Date endTime,
                                                               @Param("startorder")Integer startorder,
                                                               @Param("endorder")Integer endorder,
                                                               @Param("taskorder")Integer taskorder,
                                                               @Param("runorder")Integer runorder
    );
}