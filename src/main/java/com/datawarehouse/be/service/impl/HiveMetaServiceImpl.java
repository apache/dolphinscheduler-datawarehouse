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
import com.datawarehouse.be.dao.hivemeta.HiveMetaMapper;
import com.datawarehouse.be.pojo.dos.HiveTabParamInfoDO;
import com.datawarehouse.be.pojo.dto.ColumnDto;
import com.datawarehouse.be.pojo.vo.HiveDbTabVO;
import com.datawarehouse.be.pojo.vo.HiveTabInfoVO;
import com.datawarehouse.be.pojo.vo.HiveTabPartitionVO;
import com.datawarehouse.be.service.HiveMetaService;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.impl.BaseServiceImpl;
import org.apache.dolphinscheduler.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Service("hiveMetaService")
public class HiveMetaServiceImpl extends BaseServiceImpl implements HiveMetaService {

    private static final Logger logger = LoggerFactory.getLogger(HiveMetaServiceImpl.class);

    @Autowired
    HiveMetaMapper hiveMetaMapper;

    @Autowired
    RestTemplate restTemplate;

    @Value("${metadata_platform.url}")
    private String metaDataPlatform;

    private Map<String, Object> returnResult(Object obj) {
        Map<String, Object> result = new HashMap<>();
        putMsg(result, Status.SUCCESS);
        result.put(Constants.DATA_LIST, obj);
        return result;
    }

    @Override
    public Map<String, Object> getHiveDbs() {
        // 获取现在正在使用的 hive 数据库
        String url = metaDataPlatform + "/v1/databases/use";
        String response = restTemplate.getForObject(url,String.class);
        Map<String,Object> results = JSONObject.parseObject(response);

        // 接口访问正常
        if ((Integer)results.get("statusCodeValue") == 200) {
            List<String> databases = (List<String>) results.get("body");
            return returnResult(databases);
        }

        logger.info("获取在用的数据库列表错误");

        Map<String, Object> result = new HashMap<>();
        putMsg(result, Status.INTERNAL_SERVER_ERROR_ARGS);
        result.put(Constants.DATA_LIST, "访问元数据接口异常");
        return result;
    }

    @Override
    public Map<String, Object> getHiveTabsByDb(String dbName) {
        LinkedList<String> tables = hiveMetaMapper.getHiveTabsByDb(dbName);
        return returnResult(tables);
    }

    @Override
    public Map<String, Object> getHiveTabsByKeyword(String keyword) {
        LinkedList<HiveDbTabVO> dbTabs = hiveMetaMapper.getHiveTabsByKeyword(keyword);
        return returnResult(dbTabs);
    }


    @Override
    public Map<String, Object> getHiveTabInfo(String dbName, String tabName) {
        HiveTabInfoVO hiveTabInfoVO = new HiveTabInfoVO();
        LinkedList<HiveTabParamInfoDO> hiveTabParamInfoDOS = hiveMetaMapper.getHiveTabParamsInfo(dbName,tabName);
        // 获取修改时间和生命周期(分区天数）
        if (hiveTabParamInfoDOS != null) {
            for(HiveTabParamInfoDO hiveTabParamInfoDO : hiveTabParamInfoDOS) {
                if ("LAST_MODIFIED_TIME".equals(hiveTabParamInfoDO.getPARAM_KEY())) {
                    hiveTabInfoVO.setLastModifiedTime(hiveTabParamInfoDO.getPARAM_VALUE());
                }

                if ("PARTITION.RETENTION.PERIOD".equals(hiveTabParamInfoDO.getPARAM_KEY())) {
                    Integer period = Integer.parseInt(hiveTabParamInfoDO.getPARAM_VALUE().split("d")[0]);
                    hiveTabInfoVO.setPartitionRetentionPeriod(period);
                }
            }
        }
        return returnResult(hiveTabInfoVO);
    }

    @Override
    public Map<String, Object> getPartitionName(String dbName, String tabName) {
        LinkedList<HiveTabPartitionVO> partitionNames = hiveMetaMapper.getPartitionNames(dbName, tabName);

        Map<String, Object> result = new HashMap<>();
        if (partitionNames.size() == 1 && partitionNames.get(0).getPkName().equals("dt")) {
            putMsg(result, Status.SUCCESS);
            result.put(Constants.DATA_LIST, partitionNames.get(0));
        } else {
            // 包含多个分区字段，二级分区或分区字段不为 dt 返回错误信息
            logger.info("{}.{} 表分区字段过多/不存在或分区不为 dt", dbName, tabName);
            throw new ServiceException(HiveMetaServiceImpl.class + "-表分区字段过多/不存在或分区不为 dt");
        }
        return result;
    }


    @Override
    public Map<String, Object> getTableColumnList(String dbName, String tableName) {
        LinkedList<ColumnDto> columnList = hiveMetaMapper.getTableColumnList(dbName, tableName);

        if (columnList == null || columnList.size() == 0) {
            logger.info("请确认表是否存在:{} . {}", dbName, tableName);
        }
        return returnResult(columnList);
    }


    @Override
    public Map<String, Object> isTableExists(String dbName, String tabName) {
        Map<String, Object> isExists = new HashMap<>();
        Integer flag = hiveMetaMapper.isTheTableExsits(dbName, tabName);
        isExists.put("tabIsExists", flag == 1);

        return returnResult(isExists);
    }

    @Override
    public Map<String, String> checkPartition(String dbName, String tabName) {
        /**
         * 不能建立探分区任务
         * 0. 表不存在，不能建立任何任务
         * 可以建立探分区任务
         * 1. 表没有分区，pkName 为 dt，format 为 $[yyyyMMdd-1]
         * 2. 表以天为分区， pkName 为事实分区名， format 为 $[yyyyMMdd-1]
         * 手动解决
         * 3. 表只有一个分区，分区格式不为 $[yyyyMMdd-1]，手动解决
         * 4. 表含有多级分区，手动解决
         */
        Map<String,String> dtMap = new HashMap<>();

        // 验证表是否存在
        //TDOO:数据源问题
        Integer flag = hiveMetaMapper.isTheTableExsits(dbName, tabName);
        if (flag == 0) {
            throw new ServiceException(String.format("%s.%s 不存在", dbName, tabName));
        }

        //获取分区信息
        LinkedList<HiveTabPartitionVO> hiveTabPartitionVOS = hiveMetaMapper.getPartitionNames(dbName, tabName);

        // 无分区
        if (hiveTabPartitionVOS.isEmpty()) {
            dtMap.put(PARTITION, "dt");
            dtMap.put(FORMAT, "$[yyyyMMdd-1]");
            logger.info("hive 表 {}.{} 无分区字段", dbName, tabName);
            return dtMap;
        } else if (hiveTabPartitionVOS.size() == 1) {
            // 有一个分区，检查分区格式
            String pkName = hiveTabPartitionVOS.get(0).getPkName();

            // 检查分区的格式
            Map<String, String> partition = hiveMetaMapper.getLatestPartition(dbName, tabName).get(0);
            if (partition == null || partition.isEmpty()) {
                throw new ServiceException(String.format("%s.%s 存在分区字段，但分区信息为空,表数据可能没有产出，请验证sql的正确性,或联系研发解决",dbName, tabName));
            } else {
                if (!partition.get("PKEY_NAME").equals(pkName)) {
                    throw new ServiceException(String.format("%s.%s 真实分区字段与表元数据信息不一致",dbName, tabName));
                } else {
                    String dtFormat =  partition.get("PART_DT");
                    // 按天进行分区
                    if (dtFormat.length() == 8 && isValidDate(dtFormat)) {
                        dtMap.put(PARTITION, pkName);
                        dtMap.put(FORMAT, "$[yyyyMMdd-1]");
                        return dtMap;
                    }
                }
            }
        } else if (hiveTabPartitionVOS.size() > 1) {
            // 分区字段不唯一
            throw new ServiceException(String.format("hive表 %s.%s 包含二级或多级分区,如需建立调度任务，请联系研发解决", dbName, tabName));
        }

        // 其他 dtMap.isEmpty() == true
        return dtMap;
    }

    /**
     * 验证 string 是否符合日期格式规范
     * @param str
     * @return
     */
    private boolean isValidDate(String str) {
        boolean convertSuccess = true;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        try {
            simpleDateFormat.parse(str);
        } catch (ParseException e) {
            convertSuccess = false;
        }
        return convertSuccess;
    }

}
