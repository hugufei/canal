package com.alibaba.otter.canal.store.helper;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 相关的操作工具
 * 
 * @author jianghang 2012-6-19 下午05:49:21
 * @version 1.0.0
 */
public class CanalEventUtils {

    /**
     * 找出一个最小的position位置，相等的情况返回position1
     */
    public static LogPosition min(LogPosition position1, LogPosition position2) {
        if (position1.getIdentity().equals(position2.getIdentity())) {
            // 首先根据文件进行比较
            if (position1.getPostion().getJournalName().compareTo(position2.getPostion().getJournalName()) > 0) {
                return position2;
            } else if (position1.getPostion().getJournalName().compareTo(position2.getPostion().getJournalName()) < 0) {
                return position1;
            } else {
                // 根据offest进行比较
                if (position1.getPostion().getPosition() > position2.getPostion().getPosition()) {
                    return position2;
                } else {
                    return position1;
                }
            }
        } else {
            // 不同的主备库，根据时间进行比较
            if (position1.getPostion().getTimestamp() > position2.getPostion().getTimestamp()) {
                return position2;
            } else {
                return position1;
            }
        }
    }

    /**
     * 根据entry创建对应的Position对象
     * 事实上，parser模块解析后，已经将位置信息：binlog文件，position封装到了Event中，
     * createPosition方法只是将这些信息提取出来。
     */
    public static LogPosition createPosition(Event event) {
        //=============创建一个EntryPosition实例，提取event中的位置信息============
        EntryPosition position = new EntryPosition();

        // event所在的binlog文件
        position.setJournalName(event.getEntry().getHeader().getLogfileName());
        // event锁在binlog文件中的位置
        position.setPosition(event.getEntry().getHeader().getLogfileOffset());
        // event的创建时间
        position.setTimestamp(event.getEntry().getHeader().getExecuteTime());

        //===========将EntryPosition实例封装到一个LogPosition对象中===============
        LogPosition logPosition = new LogPosition();
        logPosition.setPostion(position);
        // LogIdentity中包含了这个event来源的mysql实力的ip地址信息
        logPosition.setIdentity(event.getLogIdentity());
        return logPosition;
    }

    /**
     * LogPosition --> EntryPosition -->
     *
     * 根据entry创建对应的Position对象
     *
     * 其中included参数用LogPosition内部维护的EntryPosition的included属性赋值。
     * 当included值为false时，会把当前get位置+1，然后开始获取Event；
     * 当为true时，则直接从当前get位置开始获取数据。
     */
    public static LogPosition createPosition(Event event, boolean included) {
        EntryPosition position = new EntryPosition();
        position.setJournalName(event.getEntry().getHeader().getLogfileName());
        position.setPosition(event.getEntry().getHeader().getLogfileOffset());
        position.setTimestamp(event.getEntry().getHeader().getExecuteTime());
        position.setIncluded(included);

        LogPosition logPosition = new LogPosition();
        logPosition.setPostion(position);
        logPosition.setIdentity(event.getLogIdentity());
        return logPosition;
    }

    /**
     * 判断当前的entry和position是否相同
     * 1） 首先比较Event的生成时间
     * 2） 接着，如果位置信息的binlog文件名或者信息不为空的话(通常不为空)，则会进行精确匹配
     *
     */
    public static boolean checkPosition(Event event, LogPosition logPosition) {
        EntryPosition position = logPosition.getPostion();
        CanalEntry.Entry entry = event.getEntry();
        // 匹配时间
        boolean result = position.getTimestamp().equals(entry.getHeader().getExecuteTime());
        // 判断是否需要根据：binlog文件+position进行比较
        boolean exactely = (StringUtils.isBlank(position.getJournalName()) && position.getPosition() == null);
        // 精确匹配
        if (!exactely) {
            result &= StringUtils.equals(entry.getHeader().getLogfileName(), position.getJournalName());
            result &= position.getPosition().equals(entry.getHeader().getLogfileOffset());
        }
        return result;
    }
}
