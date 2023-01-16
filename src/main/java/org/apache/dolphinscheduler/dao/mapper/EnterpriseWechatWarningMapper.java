package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.dolphinscheduler.dao.entity.EnterpriseWechatWarning;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @AUTHOR :  wanglj9
 * @DATE :  2021/12/14 9:21
 */

// TODO
public interface EnterpriseWechatWarningMapper extends BaseMapper<EnterpriseWechatWarning> {

    /**
     * 批量插入
     * @param enterpriseWechatWarningList
     */
    void batchInsert(List<EnterpriseWechatWarning> enterpriseWechatWarningList);

    /**
     * 批量删除
     * @param ids
     */
    void deleteByWarningIds(@Param("ids") List<Integer> ids);


    /**
     * 查询企业微信号
     * @param ids
     * @return
     */
    List<String> selectEnWechatsByIds(@Param("ids")List<Integer> ids);
}
