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
import com.datawarehouse.be.pojo.dos.CustomWarningDO;
import com.datawarehouse.be.pojo.params.AddEditProcessParam;
import com.datawarehouse.be.service.CustomWarningService;
import org.apache.dolphinscheduler.common.enums.CustomWarningTypeEnum;
import org.apache.dolphinscheduler.dao.entity.EnterpriseWechatWarning;
import org.apache.dolphinscheduler.dao.entity.PhoneWarning;
import org.apache.dolphinscheduler.dao.entity.ProcessEnwechat;
import org.apache.dolphinscheduler.dao.entity.ProcessWarning;
import org.apache.dolphinscheduler.dao.mapper.EnterpriseWechatWarningMapper;
import org.apache.dolphinscheduler.dao.mapper.PhoneWarningMapper;
import org.apache.dolphinscheduler.dao.mapper.ProcessEnwechatMapper;
import org.apache.dolphinscheduler.dao.mapper.ProcessWarningMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class CustomWarningServiceImpl implements CustomWarningService {

    private static final Logger logger = LoggerFactory.getLogger(AddEditProcessParam.class);

    @Autowired
    ProcessWarningMapper processWarningMapper;

    @Autowired
    EnterpriseWechatWarningMapper enterpriseWechatWarningMapper;

    @Autowired
    PhoneWarningMapper phoneWarningMapper;

    @Autowired
    ProcessEnwechatMapper processEnwechatMapper;

    @Value("${ehr-auth.url.simple}")
    private String ehrUrl;

    @Autowired
    RestTemplate restTemplate;

    private Pattern pattern = Pattern.compile("^^1[345678]\\d{9}$");

    private String getPhoneByEhr(String emailPrefix) {
        String url =  ehrUrl + "?userEmail=" + emailPrefix + "&size=1&page=1";
        logger.info(url);
        Map<String, Object> resultMap = JSONObject.parseObject(restTemplate.getForObject(url,String.class));
        String status = (String)resultMap.get("status");
        if ("success".equals(status)) {
            List<Map<String, Object>> data = (List<Map<String, Object>>) resultMap.get("data");
            for (Map<String, Object> usrInfo : data) {
                String phone = (String) usrInfo.get("phone");
                Matcher matcher = pattern.matcher(phone);
                boolean bool = matcher.matches();
                if (bool) {
                    return phone;
                }
            }
        }
        logger.info("{} 手机号格式不正确", emailPrefix);
        return null;
    }

    @Override
    public void addCustomWarning(Long processDefinitionId, CustomWarningDO customWarningDO) {
        if (customWarningDO == null) {
            logger.info("{} 工作流报警参数为空", processDefinitionId);
            return;
        }
        List<ProcessWarning> processWarnings = new LinkedList<>();
        List<String> userNames = customWarningDO.getUserNames();
        if (userNames != null && !userNames.isEmpty()) {
            List<ProcessEnwechat> enwechats = new ArrayList<>();
            for (String userName : userNames) {
                enwechats.add(new ProcessEnwechat(processDefinitionId, userName));
            }
            processEnwechatMapper.batchInsert(enwechats);
            if (customWarningDO.getCustomWarningType().contains(CustomWarningTypeEnum.PHONE) ) {
                List<PhoneWarning> phoneWarningList = new ArrayList<>();
                for (String userName : userNames) {
                    String phone = getPhoneByEhr(userName);
                    if ( phone != null) {
                        phoneWarningList.add(new PhoneWarning(phone));
                    }
                }
                if (!phoneWarningList.isEmpty()) {
                    phoneWarningMapper.batchInsert(phoneWarningList);
                    List<Integer> phoneWarningIds = phoneWarningList.stream()
                            .map(p -> p.getWarningId())
                            .collect(Collectors.toList());
                    for (Integer warningId : phoneWarningIds) {
                        processWarnings.add(new ProcessWarning(processDefinitionId, warningId, CustomWarningTypeEnum.PHONE.getType()));
                    }
                }
            }
            if (customWarningDO.getCustomWarningType().contains(CustomWarningTypeEnum.ENWECHAT)) {
                List<EnterpriseWechatWarning> enterpriseWechatWarningList = new ArrayList<>();
                for (String userName : userNames) {
                    enterpriseWechatWarningList.add(new EnterpriseWechatWarning(userName));
                }
                enterpriseWechatWarningMapper.batchInsert(enterpriseWechatWarningList);

                List<Integer> enWechatWarningIds = enterpriseWechatWarningList.stream()
                        .map(p -> p.getWarningId())
                        .collect(Collectors.toList());
                for (Integer warningId : enWechatWarningIds) {
                    processWarnings.add(new ProcessWarning(processDefinitionId, warningId, CustomWarningTypeEnum.ENWECHAT.getType()));
                }
            }
            if (!processWarnings.isEmpty()) {
                processWarningMapper.batchInsert(processWarnings);
                logger.info("{} 工作流添加自定义告警成功", processDefinitionId);
            }
        }
    }

    @Override
    public void deleteCustomWarning(Long processDefinitionId) {
        List<Integer> phoneWarningIds = processWarningMapper.selectByProcessIdAndWarningType(processDefinitionId, CustomWarningTypeEnum.PHONE.getType());
        List<Integer> enWechatWarningIds = processWarningMapper.selectByProcessIdAndWarningType(processDefinitionId, CustomWarningTypeEnum.ENWECHAT.getType());
        processEnwechatMapper.deleteProcessEnwechats(processDefinitionId);
        if (!phoneWarningIds.isEmpty()) {
            phoneWarningMapper.deleteByWarningIds(phoneWarningIds);
        }
        if (!enWechatWarningIds.isEmpty()) {
            enterpriseWechatWarningMapper.deleteByWarningIds(enWechatWarningIds);
        }
        processWarningMapper.deleteProcessWarningByProcessId(processDefinitionId);
        logger.info("{} 删除告警信息成功", processDefinitionId);
    }


    @Override
    public CustomWarningDO getCustomWarningParam(Long processDefinitionId) {
        CustomWarningDO customWarningDO = new CustomWarningDO();
        List<String> userNames = processEnwechatMapper.selectEnwechatsByProcessId(processDefinitionId);
        if (userNames != null && !userNames.isEmpty()) {
            customWarningDO.setUserNames(userNames);
        }
        List<Integer> enterpriseWarningIds = processWarningMapper.selectByProcessIdAndWarningType(processDefinitionId, CustomWarningTypeEnum.ENWECHAT.getType());
        List<Integer> phoneWarningIds = processWarningMapper.selectByProcessIdAndWarningType(processDefinitionId, CustomWarningTypeEnum.PHONE.getType());
        List<CustomWarningTypeEnum> customWarningTypeEnums = new ArrayList<>();
        if (enterpriseWarningIds != null && enterpriseWarningIds.size() >= 1) {
            customWarningTypeEnums.add(CustomWarningTypeEnum.ENWECHAT);
        }
        if (phoneWarningIds != null && phoneWarningIds.size() >= 1) {
            customWarningTypeEnums.add(CustomWarningTypeEnum.PHONE);
        }
        customWarningDO.setCustomWarningType(customWarningTypeEnums);
        return customWarningDO;
    }
}