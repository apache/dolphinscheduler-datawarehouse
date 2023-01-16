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

package com.datawarehouse.be.aspect;

import com.datawarehouse.be.pojo.vo.Dept;
import com.datawarehouse.be.service.UsersService;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.lang.StringUtils;
import org.apache.dolphinscheduler.api.controller.BaseController;

import org.apache.dolphinscheduler.api.security.impl.ldap.LdapService;
import org.apache.dolphinscheduler.api.service.SessionService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.dao.entity.User;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.stereotype.Component;


import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.apache.dolphinscheduler.api.enums.Status.IP_IS_EMPTY;

@Aspect
@Component
public class LoginAspect extends BaseController {
    private static final Logger logger = LoggerFactory.getLogger(LoginAspect.class);
    @Autowired
    private UsersService usersService;

    @Autowired
    private org.apache.dolphinscheduler.api.service.UsersService usersService;

    @Autowired
    LdapService ldapService;

    @Autowired
    private SessionService sessionService;

    private String FROM_FLAG = "datawarehouse";

    private String MAIL_AFTER = "@datawarehouse.com";

    @Around("execution(* org.apache.dolphinscheduler.api.controller.LoginController.login(..))")
    public Result<Map<String, String>> loginController(ProceedingJoinPoint joinPoint) throws Throwable {

        //获取方法参数值数组
        Object[] args = joinPoint.getArgs();
        logger.info("请求参数为{}", args);

        String userName = (String)args[0];
        String userPassword = (String)args[1];
        HttpServletRequest request = (HttpServletRequest)args[2];
        HttpServletResponse response = (HttpServletResponse)args[3];
        String dsziroom = request.getParameter("dsziroom");

        logger.info("login user name: {} ", userName);

        //user name check
        if (StringUtils.isEmpty(userName)) {
            return error(Status.USER_NAME_NULL.getCode(),
                    Status.USER_NAME_NULL.getMsg());
        }

        // user ip check
        String ip = getClientIpAddress(request);
        if (StringUtils.isEmpty(ip)) {
            return error(IP_IS_EMPTY.getCode(), IP_IS_EMPTY.getMsg());
        }

        // verify username and password
        Result<Map<String, String>> result=null;
        //获取用户的部门
        Dept dept  = usersService.requestGetStep(userName);
        if(dept == null){
            logger.error("没有ds使用权限");
            result = new Result<>();
            result.setCode(Status.USER_NOT_ALLOWED_DS.getCode());
            result.setMsg(Status.USER_NOT_ALLOWED_DS.getMsg());
            return result;
        }

        if(FROM_FLAG.equals(dsziroom)){
            result= authenticate(userName, userPassword, dsziroom);
        } else {
            result = authenticate(userName, userPassword, ip);
        }

        if (result.getCode() != Status.SUCCESS.getCode()) {
            return result;
        }

        response.setStatus(HttpStatus.SC_OK);
        Map<String, String> cookieMap = result.getData();
        for (Map.Entry<String, String> cookieEntry : cookieMap.entrySet()) {
            Cookie cookie = new Cookie(cookieEntry.getKey(), cookieEntry.getValue());
            cookie.setHttpOnly(true);
            response.addCookie(cookie);
        }

        logger.info("响应结果为{}",result);

        return result;
    }

    public enum Status {

        USER_NOT_ALLOWED_DS (11000,"user not allowed to use ds", "用户没有使用调度权限"),
        USER_NAME_NULL(10004, "user name is null", "用户名不能为空"),
        SUCCESS(0, "success", "成功");

        private final int code;
        private final String enMsg;
        private final String zhMsg;

        Status(int code, String enMsg, String zhMsg) {
            this.code = code;
            this.enMsg = enMsg;
            this.zhMsg = zhMsg;
        }

        public int getCode() {
            return this.code;
        }

        public String getMsg() {
            if (Locale.SIMPLIFIED_CHINESE.getLanguage().equals(LocaleContextHolder.getLocale().getLanguage())) {
                return this.zhMsg;
            } else {
                return this.enMsg;
            }
        }

        /**
         * Retrieve Status enum entity by status code.
         * @param code
         * @return
         */
        public static Optional<org.apache.dolphinscheduler.api.enums.Status> findStatusBy(int code) {
            for (org.apache.dolphinscheduler.api.enums.Status status : org.apache.dolphinscheduler.api.enums.Status.values()) {
                if (code == status.getCode()) {
                    return Optional.of(status);
                }
            }
            return Optional.empty();
        }
    }

    public Result<Map<String, String>> authenticate(String userId, String password, String extra) {
        Result<Map<String, String>> result = new Result<>();
        User user = login(userId, password, extra);
        if (user == null) {
            result.setCode(org.apache.dolphinscheduler.api.enums.Status.USER_NAME_PASSWD_ERROR.getCode());
            result.setMsg(org.apache.dolphinscheduler.api.enums.Status.USER_NAME_PASSWD_ERROR.getMsg());
            return result;
        }

        // check user state
        if (user.getState() == Flag.NO.ordinal()) {
            result.setCode(org.apache.dolphinscheduler.api.enums.Status.USER_DISABLED.getCode());
            result.setMsg(org.apache.dolphinscheduler.api.enums.Status.USER_DISABLED.getMsg());
            return result;
        }

        // create session
        String sessionId = sessionService.createSession(user, extra);
        if (sessionId == null) {
            result.setCode(org.apache.dolphinscheduler.api.enums.Status.LOGIN_SESSION_FAILED.getCode());
            result.setMsg(org.apache.dolphinscheduler.api.enums.Status.LOGIN_SESSION_FAILED.getMsg());
            return result;
        }
        logger.info("sessionId : {}", sessionId);
        result.setData(Collections.singletonMap(Constants.SESSION_ID, sessionId));
        result.setCode(org.apache.dolphinscheduler.api.enums.Status.SUCCESS.getCode());
        result.setMsg(org.apache.dolphinscheduler.api.enums.Status.LOGIN_SUCCESS.getMsg());
        return result;
    }

    public User login(String userId, String password, String extra) {
        User user = null;
        if (FROM_FLAG.equals(extra)){

            user = usersService.getUserByUserName(userId);
            if ((user == null) && (extra.equals(FROM_FLAG))) {
                user = usersService.ziroomCreateUser(ldapService.getUserType(userId), userId, userId + MAIL_AFTER);
            }
            return user;
        }
        else {
            String ldapEmail = ldapService.ldapLogin(userId, password);
            if (ldapEmail != null) {
                //check if user exist
                user = usersService.getUserByUserName(userId);
                if (user == null) {
                    user = usersService.ziroomCreateUser(ldapService.getUserType(userId), userId, ldapEmail);
                }
            }
            return user;
        }
    }

}