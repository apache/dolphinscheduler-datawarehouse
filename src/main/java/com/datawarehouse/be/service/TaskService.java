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

import com.datawarehouse.be.pojo.vo.ProcessStateVo;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.FailureStrategy;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.RunMode;
import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.common.enums.WarningType;
import org.apache.dolphinscheduler.dao.entity.User;

import java.util.Map;

public interface TaskService {

    public Map<String,Object> queryTaskListPaging(User loginUser,
                                                  String createUserName, String searchVal,
                                                  Integer pageNo, Integer pageSize);
    /**
     *
     */
    void execHivePartitionSensorAfterUpdate();

    void execDependentTaskWhenComplement(User loginUser, Long projectCode,
                                         long processDefinitionId, String cronTime, CommandType commandType,
                                         FailureStrategy failureStrategy, String startNodeList,
                                         TaskDependType taskDependType, WarningType warningType, int warningGroupId,
                                         String receivers, String receiversCc, RunMode runMode,
                                         Priority processInstancePriority, String workerGroup, Integer timeout);

    ProcessStateVo selectCounts(Integer processDefinitionId);

}
