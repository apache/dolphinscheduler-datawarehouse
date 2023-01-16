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

package com.datawarehouse.be.utils;

import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.List;

@Component
@Data
@AllArgsConstructor
public class ClickHouseUtil {

    @Autowired
    RestTemplate restTemplate;

    @Value("${clickhouse.url}")
    private String chUrl;

    @Value("${clickhouse.username}")
    private String userName;

    @Value("${clickhouse.password}")
    private String password;


    private static Logger log = LoggerFactory.getLogger(ClickHouseUtil.class);

    public ClickHouseUtil() {

    }


    /**
     * 根据传入表名获取信息
     * @param dbName
     * @param tableName
     */
    public boolean isExistClickhouseTable(String dbName, String tableName) {
        String sql ="select database,name from system.tables where database='"+ dbName + "' and name='" + tableName +"' format JSON;";
        List<Object> resultList = sendPostRequest(sql);

        if (resultList.size() > 0) {
            return true;
        }

        log.info("clickhouse 表 {}.{} 不存在", dbName, tableName);
        return false;
    }

    /**
     * 发送请求
     * @param sql
     * @return
     */
    public List<Object> sendPostRequest(String sql) {
        // 设置请求头，请求认证
        HttpHeaders headers;
        headers = new HttpHeaders();
        String authorization = userName + ":" + password;
        String basicAuth = new String(Base64.getEncoder().encode(authorization.getBytes(Charset.forName("US-ASCII"))));
        headers.set("Authorization", "Basic " + basicAuth);

        HttpEntity<MultiValueMap<String,Object>> httpEntity = new HttpEntity(sql.toString(),headers);

        ResponseEntity<String> responseString = null;
        try {
            // 执行 post sql 查询
            responseString = restTemplate.exchange(chUrl, HttpMethod.POST, httpEntity, String.class);
        } catch (HttpClientErrorException e) {
            e.printStackTrace();
            return null;
        } catch (RestClientException e) {
            e.printStackTrace();
        }

        JSONObject response =  JSONObject.parseObject(responseString.getBody());
        List<Object> dataList = (List<Object>) response.get("data");

        return dataList;
    }

}
