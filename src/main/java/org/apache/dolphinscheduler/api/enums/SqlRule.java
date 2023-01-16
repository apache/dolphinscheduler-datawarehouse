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

import org.apache.dolphinscheduler.api.strategy.HiveSqlTaskStrategy;
import org.apache.dolphinscheduler.api.strategy.SqlTaskStrategy;
import org.apache.dolphinscheduler.common.enums.DbType;

import java.util.Optional;
import java.util.stream.Stream;

public enum SqlRule {
    HIVE(DbType.HIVE, HiveSqlTaskStrategy.class);

    private DbType dbType;
    private Class<? extends SqlTaskStrategy> iTaskStrategyClass;

    public DbType getDbType() {
        return dbType;
    }

    public Class<? extends SqlTaskStrategy> getiTaskStrategyClass() {
        return iTaskStrategyClass;
    }

    SqlRule(DbType dbType, Class<? extends SqlTaskStrategy> iTaskStrategy) {
        this.dbType = dbType;
        this.iTaskStrategyClass = iTaskStrategy;
    }

    public static Optional<SqlRule> of(String dbType) {
        return Stream.of(values())
                .filter(item -> item.getDbType().name().equalsIgnoreCase(dbType))
                .findAny();
    }
}
