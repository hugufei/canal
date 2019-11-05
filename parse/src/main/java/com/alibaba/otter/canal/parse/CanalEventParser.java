package com.alibaba.otter.canal.parse;

import com.alibaba.otter.canal.common.CanalLifeCycle;

/**
 * 数据复制控制器
 * 
 * @author jianghang 2012-6-21 下午04:03:25
 * @version 1.0.0
 *
 * MysqlEventParser：伪装成单个mysql实例的slave解析binglog日志
 * GroupEventParser：伪装成多个mysql实例的slave解析binglog日志。
 * LocalBinlogEventParser：解析本地的mysql binlog。
 *
 */
public interface CanalEventParser<EVENT> extends CanalLifeCycle {

}
