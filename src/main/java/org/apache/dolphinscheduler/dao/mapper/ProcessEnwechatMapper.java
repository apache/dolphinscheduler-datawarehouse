package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.dolphinscheduler.dao.entity.ProcessEnwechat;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @AUTHOR :  wanglj9
 * @DATE :  2021/12/21 9:35
 */

public interface ProcessEnwechatMapper extends BaseMapper<ProcessEnwechat> {
    /**
     * 批量插入
     * @param processEnwechats
     */
    void batchInsert(List<ProcessEnwechat> processEnwechats);


    /**
     * 批量删除某个工作流记录的自定义报警账户
     * @param processId
     */
    void deleteProcessEnwechats(@Param("processId") Long processId);


    /**
     * 查询企业微信号
     * @param processId
     * @return
     */
    List<String> selectEnwechatsByProcessId(@Param("processId") Long processId);
}
