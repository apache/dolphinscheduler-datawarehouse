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
import com.datawarehouse.be.dao.hive.HiveTwoStaffMapper;
import com.datawarehouse.be.pojo.vo.StaffInfoVO;
import com.datawarehouse.be.service.StaffInfoService;
import org.apache.dolphinscheduler.api.enums.Status;

import org.apache.dolphinscheduler.api.service.impl.BaseServiceImpl;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.mapper.UserDepDatasourceMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Service
public class StaffInfoServiceImpl extends BaseServiceImpl implements StaffInfoService {

    private static final Logger logger = LoggerFactory.getLogger(StaffInfoServiceImpl.class);

    @Autowired
    UserDepDatasourceMapper userDepDatasourceMapper;

    @Autowired
    HiveTwoStaffMapper hiveTwoStaffMapper;

    @Autowired
    RestTemplate restTemplate;

    @Value("${email.prefix.users}")
    private String emailPrefix;


    @Override
    public Map<String, Object> getStaffInfoByHive(String keyword) {
        Map<String, Object> result = new HashMap<>();
        LinkedList<StaffInfoVO> staffInfo = hiveTwoStaffMapper.getStaffInfo(keyword);
        putMsg(result, Status.SUCCESS);
        result.put(Constants.DATA_LIST, staffInfo);
        return result;
    }

    @Override
    public Map<String, Object> getStaffInfoByEmailPre(String keyword) {
        String url = emailPrefix + "?fuzzy=" + keyword;
        Map<String, Object> result = new HashMap<>();
        Map<String, Object> resultMap = JSONObject.parseObject(restTemplate.getForObject(url,String.class));
        Integer code = (Integer) resultMap.get("code");
        List<StaffInfoVO> staffInfos = new ArrayList<>();

        if (code == 200) {
            List<Map<String,Object>> data = (List<Map<String, Object>>) resultMap.get("data");
            for (Map<String, Object> usrInfo : data) {
                StaffInfoVO staffInfoVO = new StaffInfoVO();
                staffInfoVO.setName((String) usrInfo.get("name"));
                staffInfoVO.setUserName((String) usrInfo.get("userName"));
                staffInfos.add(staffInfoVO);
            }
        } else {
            logger.info("调用邮箱前缀模糊查询接口错误");
        }
        putMsg(result, Status.SUCCESS);
        result.put(Constants.DATA_LIST, staffInfos);
        return result;
    }

    @Override
    public Map<String, Object> getUserDepDatabases(Integer userId) {
        Map<String,Object> result = new HashMap<>();
        List<String> databases = new ArrayList<>();

        List<String> userDepDatabases = userDepDatasourceMapper.getUserDepDatabases(userId);
        if (userDepDatabases != null && !userDepDatabases.isEmpty()) {
            for (String datasource : userDepDatabases) {
                JSONObject datasourceConnectionParams = JSONObject.parseObject(datasource);
                databases.add(datasourceConnectionParams.get("database").toString());
            }
        }
        putMsg(result, Status.SUCCESS);
        result.put(Constants.DATA_LIST, databases);
        return result;
    }
}
