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
import com.datawarehouse.be.pojo.vo.ProcessStateVo;
import org.apache.dolphinscheduler.dao.entity.SimpleTask;
import org.apache.dolphinscheduler.dao.entity.Task;

import org.apache.ibatis.annotations.Param;

public interface TaskMapper extends BaseMapper<Task> {

    IPage<Task> queryTaskListPaging(IPage<Task> page,
                                    @Param("searchVal") String searchVal,
                                    @Param("loginUserId") int loginUserId,
                                    @Param("createUserName") String createUserName);


    /**
     * 获取简单的任务信息
     * @param processId
     * @return
     */
    public SimpleTask querySimpleTaskByProcessId(@Param("processId") int processId);

    ProcessStateVo selectCounts(@Param("processDefinitionId") Integer processDefinitionId);

    int selectcommoncounts(@Param("processDefinitionId") Integer processDefinitionId);
}
