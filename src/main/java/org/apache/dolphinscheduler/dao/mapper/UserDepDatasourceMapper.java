package org.apache.dolphinscheduler.dao.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @AUTHOR :  wanglj9
 * @DATE :  2021/12/17 9:51
 */
public interface UserDepDatasourceMapper {

    /**
     * 获取用户所拥有连接权限的库
     * @param userName
     * @return
     */
    List<String> getUserDepDatabases(@Param("userId")Integer userId);
}
