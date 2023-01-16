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

package org.apache.dolphinscheduler.dao.entity;

import lombok.Data;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.enums.ReleaseState;

import java.util.Objects;

@Data
public class Task {

    private String taskName;

    private String tableName;

    private String endTime;

    private String startTime;

    private String userName;

    private ExecutionStatus runnerState;

    private ReleaseState taskState;

    private String stateName;

    private Long projectId;

    private String projectName;

    private int userId;

    private Long processDefinitionId;

    private int processInstaceId;

    private CommandType commandType;

    private int isRed;

    private int processType;

    private String receivers;

    private String receiversCc;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Task obj = (Task) o;

        if (processDefinitionId != obj.processDefinitionId) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(processDefinitionId);
    }
}
