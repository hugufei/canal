package com.alibaba.otter.canal.instance.spring;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.alarm.CanalAlarmHandler;
import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceSupport;
import com.alibaba.otter.canal.meta.CanalMetaManager;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.inbound.group.GroupEventParser;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.sink.CanalEventSink;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 基于spring容器启动canal实例，方便独立于manager启动
 * 
 * @author jianghang 2012-7-12 下午01:21:26
 * @author zebin.xuzb
 * @version 1.0.0
 */
public class CanalInstanceWithSpring extends CanalInstanceSupport implements CanalInstance {

    private static final Logger                    logger = LoggerFactory.getLogger(CanalInstanceWithSpring.class);
    private String                                 destination;
    private CanalEventParser                       eventParser;
    private CanalEventSink<List<CanalEntry.Entry>> eventSink;
    private CanalEventStore<Event>                 eventStore;
    private CanalMetaManager                       metaManager;
    private CanalAlarmHandler                      alarmHandler;

    public String getDestination() {
        return this.destination;
    }

    public CanalEventParser getEventParser() {
        return this.eventParser;
    }

    public CanalEventSink<List<CanalEntry.Entry>> getEventSink() {
        return this.eventSink;
    }

    public CanalEventStore<Event> getEventStore() {
        return this.eventStore;
    }

    public CanalMetaManager getMetaManager() {
        return this.metaManager;
    }

    public CanalAlarmHandler getAlarmHandler() {
        return alarmHandler;
    }

    // subscribeChange方法，主要是更新一下eventParser中的filter。
    public boolean subscribeChange(ClientIdentity identity) {
        // 如果设置了filter
        if (StringUtils.isNotEmpty(identity.getFilter())) {
            logger.info("subscribe filter change to " + identity.getFilter());
            AviaterRegexFilter aviaterFilter = new AviaterRegexFilter(identity.getFilter());
            boolean isGroup = (eventParser instanceof GroupEventParser);
            if (isGroup) {
                // 处理group的模式
                List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
                for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动
                    ((AbstractEventParser) singleEventParser).setEventFilter(aviaterFilter);
                }
            } else {
                ((AbstractEventParser) eventParser).setEventFilter(aviaterFilter);
            }
        }

        // filter的处理规则
        // a. parser处理数据过滤处理
        // b. sink处理数据的路由&分发,一份parse数据经过sink后可以分发为多份，每份的数据可以根据自己的过滤规则不同而有不同的数据
        // 后续内存版的一对多分发，可以考虑
        return true;
    }

    // 在eventParser启动后，会调用afterStartEventParser方法。
    // 这个方法内部主要是通过metaManager读取一下历史订阅过这个CanalInstance的客户端信息，然后更新一下filter。
    protected void afterStartEventParser(CanalEventParser eventParser) {
        super.afterStartEventParser(eventParser);
        // 读取一下历史订阅的filter信息
        List<ClientIdentity> clientIdentitys = metaManager.listAllSubscribeInfo(destination);
        for (ClientIdentity clientIdentity : clientIdentitys) {
            // 更新filter
            subscribeChange(clientIdentity);
        }
    }

    // 启动各个模块。启动顺序为：metaManager—>eventStore—>eventSink—>eventParser。
    public void start() {
        super.start();

        logger.info("start CannalInstance for {}-{} ", new Object[] { 1, destination });
        if (!metaManager.isStart()) {
            metaManager.start();
        }

        if (!eventStore.isStart()) {
            eventStore.start();
        }

        if (!eventSink.isStart()) {
            eventSink.start();
        }

        if (!eventParser.isStart()) {
            // 启动前执行一些操作
            beforeStartEventParser(eventParser);
            eventParser.start();
            // 启动后执行一些操作
            afterStartEventParser(eventParser);
        }

        logger.info("start successful....");
    }

    // stop顺序与启动顺序刚好相反
    public void stop() {
        logger.info("stop CannalInstance for {}-{} ", new Object[] { 1, destination });
        if (eventParser.isStart()) {
            beforeStopEventParser(eventParser);
            eventParser.stop();
            afterStopEventParser(eventParser);
        }

        if (eventSink.isStart()) {
            eventSink.stop();
        }

        if (eventStore.isStart()) {
            eventStore.stop();
        }

        if (metaManager.isStart()) {
            metaManager.stop();
        }

        if (alarmHandler.isStart()) {
            alarmHandler.stop();
        }

        // if (zkClientx != null) {
        // zkClientx.close();
        // }

        super.stop();
        logger.info("stop successful....");
    }

    // ======== setter ========
    public void setDestination(String destination) {
        this.destination = destination;
    }

    public void setEventParser(CanalEventParser eventParser) {
        this.eventParser = eventParser;
    }

    public void setEventSink(CanalEventSink<List<CanalEntry.Entry>> eventSink) {
        this.eventSink = eventSink;
    }

    public void setEventStore(CanalEventStore<Event> eventStore) {
        this.eventStore = eventStore;
    }

    public void setMetaManager(CanalMetaManager metaManager) {
        this.metaManager = metaManager;
    }

    public void setAlarmHandler(CanalAlarmHandler alarmHandler) {
        this.alarmHandler = alarmHandler;
    }

}
