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

import com.datawarehouse.be.service.SchedulerService;
import org.apache.dolphinscheduler.api.enums.Status;

import org.apache.dolphinscheduler.api.service.impl.BaseServiceImpl;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.entity.Schedule;
import org.apache.dolphinscheduler.dao.mapper.ScheduleMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SchedulerServiceImpl extends BaseServiceImpl implements SchedulerService {

    private static final Logger logger = LoggerFactory.getLogger(SchedulerServiceImpl.class);

    @Autowired
    private ScheduleMapper scheduleMapper;

    @Override
    public Map<String, Object> getCrontabByProcessDefinitionId(int processDefinitionId){
        Map<String, Object> result = new HashMap<>();
        //TODO:貌似方法发生变先注释了
        //List<Schedule> schedules = scheduleMapper.queryByProcessDefinitionId(processDefinitionId);
        List<Schedule> schedules = null;
        if (!schedules.isEmpty() && schedules.size() > 1) {
            logger.warn("scheduler num is {},Greater than 1", schedules.size());
            putMsg(result, Status.SCHEDULE_CRON_NOT_EXISTS);
            return result;
        } else if (schedules.size() == 1) {
            Schedule schedule = schedules.get(0);
            result.put(Constants.DATA_LIST, schedule.getCrontab());
        }else{
            result.put(Constants.DATA_LIST, "None");
        }
        putMsg(result, Status.SUCCESS);
        return result;
    }

}
