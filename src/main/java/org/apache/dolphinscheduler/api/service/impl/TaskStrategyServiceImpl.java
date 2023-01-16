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

package org.apache.dolphinscheduler.api.service.impl;

import org.apache.dolphinscheduler.api.enums.TaskRule;
import org.apache.dolphinscheduler.api.service.TaskStrategyService;
import org.apache.dolphinscheduler.api.strategy.ITaskStrategy;
import org.apache.dolphinscheduler.common.enums.IOType;
import org.apache.dolphinscheduler.common.enums.TaskType;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Service
public class TaskStrategyServiceImpl implements TaskStrategyService {

    @Autowired
    private Set<ITaskStrategy> taskStrategySet;


    private Optional<ITaskStrategy> getTaskStrategy(TaskNode taskNode) {
        return TaskRule.of(TaskType.valueOf(taskNode.getType()))
                .map(item -> item.getiTaskStrategyFunction())
                .map(item -> item.apply(taskNode))
                .map(item -> item.map(iTaskStrategyClass -> taskStrategySet.stream().filter(taskStrategy -> taskStrategy.getClass().equals(iTaskStrategyClass)).findAny()))
                .orElse(Optional.empty())
                .orElse(Optional.empty());
    }

    @Override
    public Map<IOType, List<String>> queryIOByTaskNode(TaskNode taskNode) {
        return getTaskStrategy(taskNode).map(item -> item.queryIOBySql(taskNode)).orElse(Collections.emptyMap());
    }
}
