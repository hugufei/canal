package com.alibaba.otter.canal.parse.index;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.parse.exception.CanalParseException;
import com.alibaba.otter.canal.protocol.position.LogPosition;

/**
 * 接口组合
 *
 *  mysql在主从同步过程中，要求slave自己维护binlog的消费进度信息。
 *  而canal伪装成slave，因此也要维护这样的信息。
 *
 *  事实上，如果读者自己搭建过mysql主从复制的话，在slave机器的data目录下，都会有一个master.info文件，
 *  这个文件的作用就是存储主库的消费binlog解析进度信息。
 *
 * @author jianghang 2012-7-7 上午10:02:02
 * @version 1.0.0
 */
public interface CanalLogPositionManager extends CanalLifeCycle {

    LogPosition getLatestIndexBy(String destination);

    void persistLogPosition(String destination, LogPosition logPosition) throws CanalParseException;
}
