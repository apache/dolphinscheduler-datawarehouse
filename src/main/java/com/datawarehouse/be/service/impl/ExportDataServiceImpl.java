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

import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.google.common.collect.Maps;
import com.datawarehouse.be.dao.hivemeta.HiveMetaMapper;
import com.datawarehouse.be.exceptions.ExportDtoException;
import com.datawarehouse.be.pojo.vo.ExportTabInfoVO;
import com.datawarehouse.be.pojo.vo.HiveDbTabVO;
import com.datawarehouse.be.service.ExportDataService;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.api.enums.Status;

import org.apache.dolphinscheduler.api.service.impl.BaseServiceImpl;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.enums.IOType;
import org.apache.dolphinscheduler.common.utils.DateUtils;

import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.entity.OutPutEntity;
import org.apache.dolphinscheduler.dao.entity.OutPutExportEntity;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinitionIO;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.mapper.ExportDataMapper;

import org.apache.dolphinscheduler.dao.mapper.ZROutPutMapper;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionIOMapper;
import org.apache.dolphinscheduler.spi.utils.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service("exportDataService")
public class ExportDataServiceImpl extends BaseServiceImpl implements ExportDataService {
    @Autowired
    ProcessDefinitionIOMapper processDefinitionIOMapper;

    @Autowired
    ZROutPutMapper outPutMapper;

    @Autowired
    ExportDataMapper exportDataMapper;

    @Autowired
    HiveMetaMapper hiveMetaMapper;

    /**
     * query export data process instance list
     */
    @Override
    public Map<String, Object> queryExportDataProcessInstanceList(String dbName, String tableName, String startTimeFilterVal, Integer pageNo, Integer pageSize) {
        //query process definition id by tableName
        Long processDefinitionId = queryProcessDefIdByTableName(dbName, tableName);
        if (processDefinitionId == null) {
            log.info("There is no work process with this table as output table!");
            return null;
        }

        //拿到processid后要判断该工作流是否有导出任务，没有的话返回null
        Page<ProcessInstance> page = new Page(pageNo, pageSize);
        PageInfo pageInfo = new PageInfo<ProcessInstance>(pageNo, pageSize);

        IPage<ProcessInstance> processInstanceList = exportDataMapper.queryExportDataProcessInstanceListPaging(
                page, processDefinitionId, startTimeFilterVal);
        List<ProcessInstance> processInstances = processInstanceList.getRecords();

        for (ProcessInstance processInstance : processInstances) {
            //Processing running time
            String[] lineStates = new String[]{
                    ExecutionStatus.RUNNING_EXECUTION.getDescp(),
                    ExecutionStatus.SUBMITTED_SUCCESS.getDescp(),
                    ExecutionStatus.NEED_FAULT_TOLERANCE.getDescp(),
                    ExecutionStatus.WAITING_THREAD.getDescp(),
                    ExecutionStatus.WAITING_DEPEND.getDescp()
            };
            ExecutionStatus state = processInstance.getState();
            if (state == null || Arrays.asList(lineStates).contains(state.getDescp())) {
                processInstance.setDuration("--");
            } else {
                processInstance.setDuration(DateUtils.format2Duration(processInstance.getStartDateTime(), processInstance.getEndDateTime()));
            }
        }
        pageInfo.setTotal((int) processInstanceList.getTotal());
        pageInfo.setTotalList(processInstances);
        return warpResult(pageInfo);
    }

    /**
     * get partition days and last update time
     */
    @Override
    public Map<String, Object> getTableDetails(String dbName, String tableName, String dataSourceName) {

        Long processDefinitionId = queryProcessDefIdByTableName(dbName, tableName);

        ExportTabInfoVO exportTabInfoVO = new ExportTabInfoVO();

        if (processDefinitionId != null) {
            //query the clickhose last update time
            LinkedList<String> ProcessInstances = exportDataMapper.queryProcessEndTime(processDefinitionId);
            String lastUpdateTime = ProcessInstances.get(0);
            exportTabInfoVO.setLastUpdateTime(lastUpdateTime);

            //query whether the exported work process exists
            LinkedList<OutPutExportEntity> zrOutPutExportEntities = exportDataMapper.queryPartitionDays(processDefinitionId, dataSourceName);

            //export work process is not exists, so we can not query the partition number of days
            if (zrOutPutExportEntities == null || zrOutPutExportEntities.size() == 0) {
                return warpResult(exportTabInfoVO);
            }

            OutPutExportEntity outPutExportEntity = zrOutPutExportEntities.get(0);
            String datasourceJson = outPutExportEntity.getDatasourceJson();
            //get  partitionDays by using FASTJSON to parse datasourceJson
            JSONObject jsonObject = JSONObject.parseObject(datasourceJson);
            String partitionDays = jsonObject.getString("retentionDays");
            exportTabInfoVO.setPartitionDays(partitionDays);
        }

        return warpResult(exportTabInfoVO);
    }

    /**
     * get process definition id
     */
    @Override
    public Map<String, Object> getProcessId(String dbName, String tableName) {
        Long processDefinitionId = queryProcessDefIdByTableName(dbName, tableName);
        Map<String, Object> result = new HashMap<>(5);
        result.put("processDefinitionId", processDefinitionId);
        return warpResult(result);
    }

    /**
     * get all dt tables on db by dbName
     */
    @Override
    public Map<String, Object> getDTHiveTable(String dbName) {
        LinkedList<String> dtHiveTable = hiveMetaMapper.getDTHiveTable(dbName);
        return warpResult(dtHiveTable);
    }

    @Override
    public Map<String, Object> getTabsByFuzzyQuery(String keyword) {
        LinkedList<HiveDbTabVO> tabsByFuzzyQuery = hiveMetaMapper.getTabsByFuzzyQuery(keyword);
        System.out.println(tabsByFuzzyQuery);
        return warpResult(tabsByFuzzyQuery);
    }

    @Override
    public Map<String, Object> verifyPermission(User loginUser, String dbName, String tableName){

        //verify parameters
        if (StringUtils.isBlank(dbName)){
            throw new ExportDtoException("导出库名不能为空~");
        }
        if (StringUtils.isBlank(tableName)){
            throw new ExportDtoException("导出表名不能为空~");
        }

        Map<String, Object> result = new HashMap<>(5);

        //gets the owner of the db.table
        String owner = hiveMetaMapper.getTableOwner(dbName, tableName);
//        String owner = "chenzy15";
        if (StringUtils.isBlank(owner)){
            throw new ExportDtoException("该表无任何负责人~");
        }

        if (loginUser.getUserName().equals(owner)){
            //loginUser is owner
            result.put("msg","当前用户是该表的负责人");
            result.put("whetherOwner","1");
        }
        else{
            //loginUser is not owner
            result.put("msg","当前用户不是该表的负责人");
            result.put("whetherOwner","0");
            result.put("tableOwner", owner);
        }
        return  warpResult(result);
    }

    /**
     * query Process_Definition_id by Output_Table_Name
     */
    private Long queryProcessDefIdByTableName(String dbName, String tableName) {

        String ioName = dbName + "." + tableName;

        Map<String, Object> conditionParam = Maps.newHashMapWithExpectedSize(1);
        conditionParam.put("db_name", dbName);
        conditionParam.put("table_name", tableName);

        List<OutPutEntity> outPutEntities = outPutMapper.selectByMap(conditionParam);

        //work process does not exist
        if (outPutEntities == null || outPutEntities.size() == 0) {
            log.info("There is no work process associated with the output table {}", ioName);
            return null;
        }

        //get process definition id
        //out_put这个表中可能存在多条条目的db.table相同
        List<Long> processIdOutList = new ArrayList<>();
        for (OutPutEntity item : outPutEntities){
            processIdOutList.add(item.getProcessId());
        }

        Long processDefinitionId = null;

        //通过definition_io表来获取由ds创建的工作流id
        //根据表名和表的类型（OUTPUT为输出表），查询与该输出表相关的工作流,注意:查到的仅仅只有一个工作流，因为输出表不允许重复
        List<ProcessDefinitionIO> processDefinitionIOS = processDefinitionIOMapper.queryByIONameAndIoType(ioName, (Integer) IOType.OUTPUT.getCode());
        ProcessDefinitionIO processDefinitionIO = null;
        Long processDefinitionIdDS = -1l;
        if (processDefinitionIOS != null && processDefinitionIOS.size() > 0){
            processDefinitionIO = processDefinitionIOS.get(0);
            processDefinitionIdDS = processDefinitionIO.getProcessDefinitionCode();
        }
        System.out.println("-------------------血缘表中的id：" + processDefinitionIdDS);
        System.out.println("-------------------输出表中的数量：" + processIdOutList.size());

        //out_put表中最多有两个dbname，tablename相同的条目
        if (processIdOutList.size() == 1 && processIdOutList.contains(processDefinitionIdDS)){
            processDefinitionId = processDefinitionIdDS;
        }

        //之前没有任何工作流，但有一个由数据导出创建的工作流
        if (processDefinitionIdDS == -1 && processIdOutList.size() == 1){
            processDefinitionId = processIdOutList.get(0);
        }

        //存在一个由ds创建的无导出任务的工作流和一个由数据导出创建的工作流
        if (processIdOutList.size() != 1){
            for (Long id : processIdOutList){
                if (id != processDefinitionIdDS){
                    processDefinitionId = id;
                    break;
                }
            }
        }
        return processDefinitionId;
    }

    /**
     * warp the output
     */
    private Map<String, Object> warpResult(Object obj) {
        Map<String, Object> result = new HashMap<>(5);
        putMsg(result, Status.SUCCESS);
        result.put(Constants.DATA_LIST, obj);
        return result;
    }
}
