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

import org.apache.dolphinscheduler.api.strategy.HiveSqlTaskStrategy;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinitionIO;
import org.apache.dolphinscheduler.common.enums.IOType;
import com.datawarehouse.be.service.LineageService;
import com.datawarehouse.be.service.ProcessDefinitionIOService;
import org.apache.dolphinscheduler.api.enums.Status;

import org.apache.dolphinscheduler.api.service.impl.BaseServiceImpl;
import org.apache.dolphinscheduler.common.task.sql.SqlParameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class LineageServiceImpl extends BaseServiceImpl implements LineageService {

    @Autowired
    private HiveSqlTaskStrategy hiveSqlTaskStrategy;

    @Autowired
    private ProcessDefinitionIOService ziroomProcessDefinitionIOService;

    @Override
    public Map<String,Object> querySqlLineage(List<SqlParameters> sqlParameters) {

        Map<String, Object> result = new HashMap<>();

        Map<IOType, Set<String>> ioMap = sqlParameters.stream()
                .map(sqlParameter -> {
                    Map<IOType, List<String>> ioTypeListMap = hiveSqlTaskStrategy.queryIOBySqlParameters(sqlParameter);
                    return ioTypeListMap;
                }).filter(item -> !item.isEmpty())
                .flatMap(item -> item.entrySet().stream())
                .flatMap(item -> item.getValue().stream().collect(Collectors.toMap(v -> v, v -> item.getKey(), (k1, k2) -> k2)).entrySet().stream())
                .collect(Collectors.groupingBy(item -> item.getValue(), Collectors.mapping(item -> item.getKey(), Collectors.toSet())));
        Set<String> outputs = ioMap.getOrDefault(IOType.OUTPUT, Collections.emptySet());
        Set<String> inputs = ioMap.getOrDefault(IOType.INPUT, Collections.emptySet());
        inputs.stream().filter(item -> outputs.contains(item)).forEach(item -> inputs.remove(item));
        Map<String, Object> stringObjectMap = ziroomProcessDefinitionIOService.queryByIO(IOType.OUTPUT, new ArrayList<>(inputs));
        List<ProcessDefinitionIO> processDefinitionIOS = (List<ProcessDefinitionIO>)stringObjectMap.get(Constants.DATA_LIST);
        putMsg(result, Status.SUCCESS);
        result.put(Constants.DATA_LIST, processDefinitionIOS);
        return result;
    }

}
