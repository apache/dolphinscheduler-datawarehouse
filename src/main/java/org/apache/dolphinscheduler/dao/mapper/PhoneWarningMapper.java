package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.dolphinscheduler.dao.entity.PhoneWarning;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface PhoneWarningMapper extends BaseMapper<PhoneWarning> {

    int batchInsert(List<PhoneWarning> phoneWarningList);

    void deleteByWarningIds(@Param("ids") List<Integer> ids);

    List<String> selectPhonesByIds(@Param("ids")List<Integer> ids);

    int selectphonestate(@Param("processId") Integer processId,@Param("type") int type);
}
