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

import org.apache.dolphinscheduler.dao.entity.ProcessDefinitionIO;
import org.apache.dolphinscheduler.common.enums.IOType;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Set;

/**
 * process definition mapper interface
 */
@Repository
public interface ProcessDefinitionIOMapper extends BaseMapper<ProcessDefinitionIO> {

    /**
     * verify process definition io by id and version
     */
    int verifyByDefineIdAndVersionAndType(@Param("processDefinitionCode") int processDefinitionCode,
                                          @Param("processDefinitionVersion") long processDefinitionVersion,
                                          @Param("ioType") IOType ioType);


    List<ProcessDefinitionIO> queryByIOTypeAndIONames(@Param("ioType") int ioType, @Param("ioNames")List<String> ioNames);
    List<ProcessDefinitionIO> queryByIOType(@Param("ioType") int ioType);

    void deleteByDefineId(@Param("processDefinitionCode") Long processDefinitionCode);

    List<ProcessDefinitionIO> queryByProcessDefinitionIdAndIoType(@Param("processDefinitionCode") Long processDefinitionCode,
                                                                  @Param("ioType") Integer ioType);


    List<ProcessDefinitionIO> queryByIOName(@Param("ioName") String ioName);

    List<ProcessDefinitionIO> queryByIONameAndIoType(@Param("ioName") String ioName, @Param("ioType") Integer ioType);

    /**
     * 通过输入输出流查找工作流
     * @param ioName
     * @param ioType
     * @return
     */
    List<ProcessDefinitionIO> queryByIoNamesAndIoType(@Param("ioNames") Set<String> ioName, @Param("ioType") Integer ioType);

    /**
     * 通过工作流ids 查询 ioNames
     * @param ids
     * @param ioType
     * @return
     */
    List<ProcessDefinitionIO> queryByProcessIdsAndIoType(@Param("processIds") Set<Long> ids, @Param("ioType") Integer ioType);

}
