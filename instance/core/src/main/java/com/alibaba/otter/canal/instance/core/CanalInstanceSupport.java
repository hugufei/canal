package com.alibaba.otter.canal.instance.core;

import java.util.List;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.ha.HeartBeatHAController;
import com.alibaba.otter.canal.parse.inbound.AbstractEventParser;
import com.alibaba.otter.canal.parse.inbound.group.GroupEventParser;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.index.CanalLogPositionManager;

/**
 * @author zebin.xuzb 2012-10-17 下午3:12:34
 * @version 1.0.0
 */
public abstract class CanalInstanceSupport extends AbstractCanalLifeCycle {

    // 该方法的作用是eventParser启动前做的一些特殊处理。
    // 1）首先会判断eventParser的类型是否是GroupEventParser，在前面我已经介绍过，这是为了处理分库分表的情况。
    // 如果是，循环其包含的所有CanalEventParser，依次调用startEventParserInternal；
    // 2）否则直接调用startEventParserInternal
    protected void beforeStartEventParser(CanalEventParser eventParser) {
        // 1、判断eventParser的类型是否是GroupEventParser
        boolean isGroup = (eventParser instanceof GroupEventParser);
        // 2、如果是GroupEventParser，则循环启动其内部包含的每一个CanalEventParser，依次调用startEventParserInternal方法
        if (isGroup) {
            // 处理group的模式
            List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
            for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动
                startEventParserInternal(singleEventParser, true);
            }
        } else {
            //如果不是，说明是一个普通的CanalEventParser，直接调用startEventParserInternal方法
            startEventParserInternal(eventParser, false);
        }
    }

    // around event parser
    protected void afterStartEventParser(CanalEventParser eventParser) {
        // noop
    }

    // around event parser
    protected void beforeStopEventParser(CanalEventParser eventParser) {
        // noop
    }

    protected void afterStopEventParser(CanalEventParser eventParser) {

        boolean isGroup = (eventParser instanceof GroupEventParser);
        if (isGroup) {
            // 处理group的模式
            List<CanalEventParser> eventParsers = ((GroupEventParser) eventParser).getEventParsers();
            for (CanalEventParser singleEventParser : eventParsers) {// 需要遍历启动
                stopEventParserInternal(singleEventParser);
            }
        } else {
            stopEventParserInternal(eventParser);
        }
    }

    /**
     * 初始化单个eventParser，不需要考虑group
     *
     * 其内部会启动CanalLogPositionManager和CanalHAController。
     *
     */
    protected void startEventParserInternal(CanalEventParser eventParser, boolean isGroup) {
        // 1 、启动CanalLogPositionManager
        if (eventParser instanceof AbstractEventParser) {
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            // 首先启动log position管理器
            CanalLogPositionManager logPositionManager = abstractEventParser.getLogPositionManager();
            if (!logPositionManager.isStart()) {
                logPositionManager.start();
            }
        }
        // 2 、启动CanalHAController
        if (eventParser instanceof MysqlEventParser) {
            MysqlEventParser mysqlEventParser = (MysqlEventParser) eventParser;
            CanalHAController haController = mysqlEventParser.getHaController();
            if (haController instanceof HeartBeatHAController) {
                ((HeartBeatHAController) haController).setCanalHASwitchable(mysqlEventParser);
            }
            if (!haController.isStart()) {
                haController.start();
            }
        }
    }

    protected void stopEventParserInternal(CanalEventParser eventParser) {
        if (eventParser instanceof AbstractEventParser) {
            AbstractEventParser abstractEventParser = (AbstractEventParser) eventParser;
            // 首先启动log position管理器
            CanalLogPositionManager logPositionManager = abstractEventParser.getLogPositionManager();
            if (logPositionManager.isStart()) {
                logPositionManager.stop();
            }
        }

        if (eventParser instanceof MysqlEventParser) {
            MysqlEventParser mysqlEventParser = (MysqlEventParser) eventParser;
            CanalHAController haController = mysqlEventParser.getHaController();
            if (haController.isStart()) {
                haController.stop();
            }
        }
    }

}
