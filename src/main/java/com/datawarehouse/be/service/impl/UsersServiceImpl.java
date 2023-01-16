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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.datawarehouse.be.pojo.vo.Dept;
import com.datawarehouse.be.service.UsersService;
import org.apache.dolphinscheduler.api.enums.Status;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.UserType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.Group;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.entity.UserGroup;
import org.apache.dolphinscheduler.dao.mapper.GroupMapper;
import org.apache.dolphinscheduler.dao.mapper.UserGroupMapper;
import org.apache.dolphinscheduler.dao.mapper.UserMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import java.util.Date;
import java.util.Map;

@Service
@Configuration
public class UsersServiceImpl implements UsersService {

    private static final Logger logger = LoggerFactory.getLogger(UsersServiceImpl.class);

    @Value("${dmpserver.auth-url:http://dmpserver.kp.ziroom.com/databaseAuth/user/getGroupByUser?userName=}")
    private String authUrl;

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private org.apache.dolphinscheduler.api.service.UsersService usersService;

    @Autowired
    private GroupMapper groupMapper;

    @Autowired
    private UserGroupMapper userGroupMapper;

    @Autowired
    private RestTemplate restTemplate;

    @Value("${sensor.project.id}")
    public int COMMON_PEOJECT_ID;

    @Transactional(rollbackFor = Exception.class)
    public User ziroomCreateUser(UserType userType, String userId, String email) {
        User user = buildUser(userType, userId, email);
        try{
            boolean isSuccess = initUserGroupSource(user);

            if(!isSuccess){
                throw new RuntimeException("user" + userId + "初始化用户项目资源失败！");
            }
        }catch (Exception e){
           throw new RuntimeException("user" + userId + "初始化用户项目资源失败！", e);
        }
        return user;
    }

    private boolean initUserGroupSource(User user){
        String userEmailPre  = user.getUserName();
        //获取用户的部门
        Dept dept  = requestGetStep(userEmailPre);
        if(dept == null){
            logger.error("从权限系统获取用户的组织信息失败！");
            return false;
        }
        String groupId = dept.getDeptCode();
        //获取用户需要初始化的项目、资源、数据源、udf、租户信息
        //Group group = groupMapper.selectById(groupId);
        Group group = groupMapper.queryGroupInfo(groupId);
        if(group == null){
            logger.error("根据部门信息获取项目初始化资源、数据源、udf、租户信息失败！");
            return false;
        }
        //初始化租户
        int tenantId = group.getTenantId();
        user.setTenantId(tenantId);
        userMapper.insert(user);
        int userId = user.getId();
        //虚拟管理员
        User adminUser = new User();
        adminUser.setUserType(UserType.ADMIN_USER);
        //项目授权初始化
        int projectId = group.getProjectId();
        StringBuffer  projectIds = new StringBuffer();
        projectIds.append(projectId).append(",").append(COMMON_PEOJECT_ID);
        logger.info("ids:"+projectIds.toString());
        Map<String, Object> grantResult = usersService.grantProject(adminUser, userId, projectIds.toString());
        if(isNotSuccessBoolean(grantResult)){
            logger.error("项目授权初始化失败！，因为ids：" + projectId + grantResult.get("msg") + "！");
            return false;
        }
        //资源授权初始化
        String sourceIds = group.getInitSourceIds();
        grantResult = usersService.grantResources(adminUser, userId, sourceIds);
        if(isNotSuccessBoolean(grantResult)){
            logger.error("资源授权初始化失败，因为ids：" + sourceIds + grantResult.get("msg") + "！");
            return false;
        }
        //UDF授权初始化
        String udfIds = group.getInitUdfIds();
        grantResult = usersService.grantUDFFunction(adminUser, userId, udfIds);
        if(isNotSuccessBoolean(grantResult)){
            logger.error("UDF授权初始化失败，因为ids：" + udfIds + grantResult.get("msg") + "！");
            return false;
        }
        //数据源授权初始化
        String datasourceIds = group.getInitDatasourceIds();
        grantResult = usersService.grantDataSource(adminUser, userId, datasourceIds);
        if(isNotSuccessBoolean(grantResult)){
            logger.error("数据源授权初始化失败，因为ids：" + datasourceIds + grantResult.get("msg") + "！");
            return false;
        }
        //记录初始化授权日志
        UserGroup userGroup = buildUserGroup(userEmailPre, group, tenantId, userId, projectId, sourceIds, udfIds, datasourceIds);
        userGroupMapper.insert(userGroup);
        return true;
    }

    public Dept requestGetStep(String userEmailPre){
        Dept dept = null;
        //获取指定用户部门信息
        String url= authUrl + userEmailPre;
        logger.info("authUrl:" + url);
        ResponseEntity<String> result = restTemplate.getForEntity(url,String.class);
        HttpStatus status = result.getStatusCode();
        if(status.is2xxSuccessful() && result.hasBody()){
            String body = result.getBody();
            JSONObject resultObj = JSONUtils.parseObject(body, JSONObject.class);
            logger.info("authResponse:" + resultObj);
            if(resultObj.containsKey("code") && resultObj.getString("code").equals("0")){
                JSONArray data = resultObj.getJSONArray("data");
                if(data != null && data.size() > 0){
                    dept =  new Dept();
                    String code = data.getString(0);
                    dept.setDeptCode(code);
                    dept.setDeptName(code);
                    return dept;
                }
            }
        }
        logger.info(result.getBody());
        return dept;
    }

    private User buildUser(UserType userType, String userId, String email) {
        User user = new User();
        Date now = new Date();
        user.setUserName(userId);
        user.setEmail(email);
        // create general users, administrator users are currently built-in
        user.setUserType(userType);
        user.setCreateTime(now);
        user.setUpdateTime(now);
        user.setQueue("");
        user.setState(1);
        return user;
    }
    private boolean isNotSuccessBoolean(Map<String, Object> grantResult) {
        return !grantResult.get(Constants.STATUS).equals(Status.SUCCESS);
    }

    private UserGroup buildUserGroup(String userEmailPre, Group group, int tenantId, int userId, int projectId, String sourceIds, String udfIds, String datasourceIds) {
        Date now = new Date();
        UserGroup userGroup = new UserGroup();
        userGroup.setUserId(userId);
        userGroup.setUserMailPre(userEmailPre);
        userGroup.setProjectId(projectId);
        userGroup.setTenantId(tenantId);
        userGroup.setInitSourceIds(sourceIds);
        userGroup.setInitUdfIds(udfIds);
        userGroup.setInitDatasourceIds(datasourceIds);
        userGroup.setGroupId(group.getGroupId());
        userGroup.setGroupName(group.getGroupName());
        userGroup.setCreateTime(now);
        userGroup.setUpdateTime(now);
        return userGroup;
    }
}
