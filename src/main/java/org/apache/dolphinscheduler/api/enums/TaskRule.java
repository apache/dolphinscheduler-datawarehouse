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

package org.apache.dolphinscheduler.api.enums;

import org.apache.dolphinscheduler.api.strategy.ITaskStrategy;
import org.apache.dolphinscheduler.common.enums.TaskType;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.task.sql.SqlParameters;
import org.apache.dolphinscheduler.common.utils.JSONUtils;

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public enum TaskRule {

    SQL_RULE(TaskType.SQL, taskNode -> {
        SqlParameters sqlParameters = JSONUtils.parseObject(taskNode.getParams(), SqlParameters.class);
        return SqlRule.of(sqlParameters.getType()).map(item -> item.getiTaskStrategyClass());
    });

    private TaskType taskType;

    private Function<TaskNode, Optional<Class<? extends ITaskStrategy>>> iTaskStrategyFunction;

    public TaskType getTaskType() {
        return taskType;
    }

    public Function<TaskNode, Optional<Class<? extends ITaskStrategy>>> getiTaskStrategyFunction() {
        return iTaskStrategyFunction;
    }

    TaskRule(TaskType taskType, Function<TaskNode, Optional<Class<? extends ITaskStrategy>>> iTaskStrategyFunction) {
        this.taskType = taskType;
        this.iTaskStrategyFunction = iTaskStrategyFunction;
    }

    public static Optional<TaskRule> of(TaskType taskType) {
        return Stream.of(values())
                .filter(item -> item.getTaskType() == taskType)
                .findAny();
    }

}
