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

package com.datawarehouse.be.service.impl;

import com.google.common.collect.Maps;
import com.datawarehouse.be.pojo.vo.ProcessInfoVO;
import com.datawarehouse.be.service.ProcessDefinitionService;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.ProjectService;
import org.apache.dolphinscheduler.api.service.SchedulerService;
import org.apache.dolphinscheduler.api.service.impl.BaseServiceImpl;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.task.sql.SqlParameters;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.*;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.apache.dolphinscheduler.dao.mapper.TaskDefinitionMapper;
import org.apache.dolphinscheduler.dao.mapper.ZRScheduleMapper;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class ProcessDefinitionServiceImpl extends BaseServiceImpl implements ProcessDefinitionService {

    @Autowired
    private ProjectMapper projectMapper;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private ProcessDefinitionMapper processDefineMapper;

    @Autowired
    private SchedulerService schedulerService;

    @Autowired
    private ZRScheduleMapper zrScheduleMapper;

    @Autowired
    private TaskDefinitionMapper taskDefinitionMapper;

    @Autowired
    private ProcessDefinitionMapper processDefinitionMapper;

    @Autowired
    private ProcessService processService;

    @Override
    public Map<String, Object> queryZiroomProcessDefinitionById(User user,
                                                                String projectName,
                                                                Long processId){
        Map<String, Object> result = new HashMap<>();
        Project project = projectMapper.queryByName(projectName);
        ProcessDefinition processDefinition = processDefineMapper.queryByCode(processId);
        List<ProcessTaskRelation> processTaskRelations = processService.findRelationByCode(processDefinition.getCode(), processDefinition.getVersion());
        List<TaskDefinitionLog> taskDefinitionLogs = processService.getTaskDefineLogListByRelation(processTaskRelations);
        Optional<TaskDefinitionLog> res = taskDefinitionLogs.stream().filter(x -> x.getTaskType().equals("SQL")).findFirst();
        String sql = "";
        if (res.isPresent()){
            SqlParameters sqlParameters = JSONUtils.parseObject(res.get().getTaskParams(), SqlParameters.class);
            sql = sqlParameters.getSql();
        }
        ProcessInfoVO processInfoVO = new ProcessInfoVO();
        processInfoVO.setId(processDefinition.getId());
        processInfoVO.setCreateTime(processDefinition.getCreateTime());
        processInfoVO.setDescription(processDefinition.getDescription());
        processInfoVO.setName(processDefinition.getName());
        processInfoVO.setProjectId((int) processDefinition.getProjectCode());
        processInfoVO.setReleaseState(processDefinition.getReleaseState());
        processInfoVO.setUpdateTime(processDefinition.getUpdateTime());
        processInfoVO.setVersion(processDefinition.getVersion());

        Map<String, Object> crontabByProcessDefinitionId = schedulerService.queryScheduleList(user,project.getCode());
        if(crontabByProcessDefinitionId.get(Constants.DATA_LIST) == null){
            return crontabByProcessDefinitionId;
        }
        // 获取工作流执行计划
        Map<String, Object> conditionMap = Maps.newHashMapWithExpectedSize(1);
        conditionMap.put("process_id", processId);
        List<ScheduleEntity> schedules = zrScheduleMapper.selectByMap(conditionMap);
        String cycle = "";
        String dateValue = "";
        String timeValue = "";
        if (CollectionUtils.isNotEmpty(schedules)) {
            cycle = schedules.stream().findFirst().map(e -> e.getCycle()).orElse("");
            dateValue = schedules.stream().findFirst().map(e -> e.getDateValue()).orElse("");
            timeValue = schedules.stream().findFirst().map(e -> e.getTimeValue()).orElse("");
        }
        processInfoVO.setCron(cycle + " " + dateValue + " " + timeValue);
        processInfoVO.setSql(sql);
        processInfoVO.setDescription(processDefinition.getDescription());
        processInfoVO.setToStream("hive");
        result.put(Constants.DATA_LIST, processInfoVO);
        putMsg(result, Status.SUCCESS);
        return result;
    }
}
