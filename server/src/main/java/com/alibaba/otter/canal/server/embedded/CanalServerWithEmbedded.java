package com.alibaba.otter.canal.server.embedded;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.alibaba.otter.canal.server.CanalServer;
import com.alibaba.otter.canal.server.exception.CanalServerException;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MigrateMap;

/**
 * 嵌入式版本实现
 *
 * @author jianghang 2012-7-12 下午01:34:00
 * @author zebin.xuzb
 * @version 1.0.0
 */
public class CanalServerWithEmbedded extends AbstractCanalLifeCycle implements CanalServer, com.alibaba.otter.canal.server.CanalService {

    private static final Logger logger = LoggerFactory.getLogger(CanalServerWithEmbedded.class);

    // key为destination，value为对应的CanalInstance
    // 会根据客户端请求携带的destination参数将其转发到对应的CanalInstance上去处理。
    private Map<String, CanalInstance> canalInstances;

    // private Map<ClientIdentity, Position> lastRollbackPostions;
    private CanalInstanceGenerator canalInstanceGenerator;

    public void start() {
        super.start();

        canalInstances = MigrateMap.makeComputingMap(new Function<String, CanalInstance>() {

            public CanalInstance apply(String destination) {
                return canalInstanceGenerator.generate(destination);
            }
        });

        // lastRollbackPostions = new MapMaker().makeMap();
    }

    public void stop() {
        super.stop();
        for (Map.Entry<String, CanalInstance> entry : canalInstances.entrySet()) {
            try {
                CanalInstance instance = entry.getValue();
                if (instance.isStart()) {
                    try {
                        String destination = entry.getKey();
                        MDC.put("destination", destination);
                        entry.getValue().stop();
                        logger.info("stop CanalInstances[{}] successfully", destination);
                    } finally {
                        MDC.remove("destination");
                    }
                }
            } catch (Exception e) {
                logger.error(String.format("stop CanalInstance[%s] has an error", entry.getKey()), e);
            }
        }
    }

    public void start(final String destination) {
        final CanalInstance canalInstance = canalInstances.get(destination);
        if (!canalInstance.isStart()) {
            try {
                MDC.put("destination", destination);
                canalInstance.start();
                logger.info("start CanalInstances[{}] successfully", destination);
            } finally {
                MDC.remove("destination");
            }
        }
    }

    public void stop(String destination) {
        CanalInstance canalInstance = canalInstances.remove(destination);
        if (canalInstance != null) {
            if (canalInstance.isStart()) {
                try {
                    MDC.put("destination", destination);
                    canalInstance.stop();
                    logger.info("stop CanalInstances[{}] successfully", destination);
                } finally {
                    MDC.remove("destination");
                }
            }
        }
    }

    public boolean isStart(String destination) {
        return canalInstances.containsKey(destination) && canalInstances.get(destination).isStart();
    }

    /**
     * 客户端订阅，重复订阅时会更新对应的filter信息
     *
     * subscribe主要用于处理客户端的订阅请求，目前情况下，一个CanalInstance只能由一个客户端订阅，不过可以重复订阅。订阅主要的处理步骤如下：
     *
     * 1、根据客户端要订阅的destination，找到对应的CanalInstance
     * 2、通过这个CanalInstance的CanalMetaManager组件记录下有客户端订阅。
     * 3、获取客户端当前订阅位置(Position)。首先尝试从CanalMetaManager中获取，CanalMetaManager 中记录了某个client当前订阅binlog的位置信息。
     * 如果是第一次订阅，肯定无法获取到这个位置，则尝试从CanalEventStore中获取第一个binlog的位置。
     * 从CanalEventStore中获取binlog位置信息的逻辑是：CanalInstance一旦启动，就会立刻去拉取binlog，存储到CanalEventStore中，
     * 在第一次订阅的情况下，CanalEventStore中的第一条binlog的位置，就是当前客户端当前消费的开始位置。
     *
     * 4、通知CanalInstance订阅关系变化
     *
     */
    @Override
    public void subscribe(ClientIdentity clientIdentity) throws CanalServerException {
        // 1、根据客户端要订阅的destination，找到对应的CanalInstance
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        if (!canalInstance.getMetaManager().isStart()) {
            canalInstance.getMetaManager().start();
        }

        // 2、通过CanalInstance的CanalMetaManager组件进行元数据管理，记录一下当前这个CanalInstance有客户端在订阅
        canalInstance.getMetaManager().subscribe(clientIdentity); // 执行一下meta订阅

        // 3、获取客户端当前订阅的binlog位置(Position)，首先尝试从CanalMetaManager中获取
        Position position = canalInstance.getMetaManager().getCursor(clientIdentity);

        // 3.1 如果是第一次订阅，尝试从CanalEventStore中获取第一个binlog的位置，作为客户端订阅开始的位置。
        if (position == null) {
            position = canalInstance.getEventStore().getFirstPosition();// 获取一下store中的第一条
            if (position != null) {
                canalInstance.getMetaManager().updateCursor(clientIdentity, position); // 更新一下cursor
            }
            logger.info("subscribe successfully, {} with first position:{} ", clientIdentity, position);
        } else {
            logger.info("subscribe successfully, use last cursor position:{} ", clientIdentity, position);
        }

        // 4 通知下订阅关系变化
        canalInstance.subscribeChange(clientIdentity);
    }

    /**
     * 取消订阅
     *
     * unsubscribe方法主要用于取消订阅关系。
     * 在下面的代码中，我们可以看到，其实就是找到CanalInstance对应的CanalMetaManager，调用其unsubscribe取消这个订阅记录。
     * 需要注意的是，取消订阅并不意味着停止CanalInstance。
     *
     * 某个客户端取消了订阅，还会有新的client来订阅这个CanalInstance，所以不能停。
     *
     */
    @Override
    public void unsubscribe(ClientIdentity clientIdentity) throws CanalServerException {
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        canalInstance.getMetaManager().unsubscribe(clientIdentity); // 执行一下meta订阅

        logger.info("unsubscribe successfully, {}", clientIdentity);
    }

    /**
     * 查询所有的订阅信息
     *
     * 这一个管理方法，其作用是列出订阅某个destination的所有client。
     * 这里返回的是一个List<ClientIdentity>，不过我们已经多次提到，目前一个destination只能由一个client订阅。
     * 这里之所以返回一个list，是canal原先计划要支持多个client订阅同一个destination。
     * 不过，这个功能一直没有实现。所以List中，实际上只会包含一个ClientIdentity。
     */
    public List<ClientIdentity> listAllSubscribe(String destination) throws CanalServerException {
        CanalInstance canalInstance = canalInstances.get(destination);
        return canalInstance.getMetaManager().listAllSubscribeInfo(destination);
    }


    /**
     * 获取数据
     *
     * 注意： meta获取和数据的获取需要保证顺序性，优先拿到meta的，一定也会是优先拿到数据，所以需要加同步.
     * (不能出现先拿到meta，拿到第二批数据，这样就会导致数据顺序性出现问题)
     */

    @Override
    public Message get(ClientIdentity clientIdentity, int batchSize) throws CanalServerException {
        return get(clientIdentity, batchSize, null, null);
    }

    /**
     * 获取数据，可以指定超时时间.
     *
     * 与getWithoutAck主要流程完全相同，唯一不同的是，在返回数据给用户前，直接进行了ack，而不管客户端消费是否成功。
     *
     *
     * <pre>
     * 几种case:
     * a. 如果timeout为null，则采用tryGet方式，即时获取
     * b. 如果timeout不为null
     *    1. timeout为0，则采用get阻塞方式，获取数据，不设置超时，直到有足够的batchSize数据才返回
     *    2. timeout不为0，则采用get+timeout方式，获取数据，超时还没有batchSize足够的数据，有多少返回多少
     *
     * 注意： meta获取和数据的获取需要保证顺序性，优先拿到meta的，一定也会是优先拿到数据，所以需要加同步. (不能出现先拿到meta，拿到第二批数据，这样就会导致数据顺序性出现问题)
     * </pre>
     */
    @Override
    public Message get(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit)
            throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        checkSubscribe(clientIdentity);
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        synchronized (canalInstance) {
            // 获取到流式数据中的最后一批获取的位置
            PositionRange<LogPosition> positionRanges = canalInstance.getMetaManager().getLastestBatch(clientIdentity);

            if (positionRanges != null) {
                throw new CanalServerException(String.format("clientId:%s has last batch:[%s] isn't ack , maybe loss data",
                        clientIdentity.getClientId(),
                        positionRanges));
            }

            Events<Event> events = null;
            Position start = canalInstance.getMetaManager().getCursor(clientIdentity);
            events = getEvents(canalInstance.getEventStore(), start, batchSize, timeout, unit);

            if (CollectionUtils.isEmpty(events.getEvents())) {
                logger.debug("get successfully, clientId:{} batchSize:{} but result is null", new Object[]{
                        clientIdentity.getClientId(), batchSize});
                return new Message(-1, new ArrayList<Entry>()); // 返回空包，避免生成batchId，浪费性能
            } else {
                // 记录到流式信息
                Long batchId = canalInstance.getMetaManager().addBatch(clientIdentity, events.getPositionRange());
                List<Entry> entrys = Lists.transform(events.getEvents(), new Function<Event, Entry>() {

                    public Entry apply(Event input) {
                        return input.getEntry();
                    }
                });

                logger.info("get successfully, clientId:{} batchSize:{} real size is {} and result is [batchId:{} , position:{}]",
                        clientIdentity.getClientId(),
                        batchSize,
                        entrys.size(),
                        batchId,
                        events.getPositionRange());
                // 直接提交ack
                ack(clientIdentity, batchId);
                return new Message(batchId, entrys);
            }
        }
    }

    /**
     * 不指定 position 获取事件。canal 会记住此 client 最新的 position。 <br/>
     * 如果是第一次 fetch，则会从 canal 中保存的最老一条数据开始输出。
     *
     * <pre>
     * 注意： meta获取和数据的获取需要保证顺序性，优先拿到meta的，一定也会是优先拿到数据，所以需要加同步. (不能出现先拿到meta，拿到第二批数据，这样就会导致数据顺序性出现问题)
     * </pre>
     */
    @Override
    public Message getWithoutAck(ClientIdentity clientIdentity, int batchSize) throws CanalServerException {
        return getWithoutAck(clientIdentity, batchSize, null, null);
    }

    /**
     * getWithoutAck方法用于客户端获取binlog消息 ，一个获取一批(batch)的binlog，canal会为这批binlog生成一个唯一的batchId。
     * 1）客户端如果消费成功，则调用ack方法对这个批次进行确认。
     * 2）如果失败的话，可以调用rollback方法进行回滚。
     *
     * PS:
     * 客户端可以连续多次调用getWithoutAck方法来获取binlog，在ack的时候，需要按照获取到binlog的先后顺序进行ack。
     * 如果后面获取的binlog被ack了，那么之前没有ack的binlog消息也会自动被ack。
     *
     * 大致工作步骤:
     * 1) 根据destination找到要从哪一个CanalInstance中获取binlog消息。
     *
     * 2) 确定从哪一个位置(Position)开始继续消费binlog。通常情况下，这个信息是存储在CanalMetaManager中。
     * 特别的，在第一次获取的时候，CanalMetaManager 中还没有存储任何binlog位置信息。
     * 此时CanalEventStore中存储的第一条binlog位置，则就是client开始消费的位置。
     *
     * 3) 根据Position从CanalEventStore中获取binlog。为了尽量提高效率，一般一次获取一批binlog，而不是获取一条。
     * ① 这个批次的大小(batchSize)由客户端指定。
     * ② 同时客户端可以指定超时时间，在超时时间内，如果获取到了batchSize的binlog，会立即返回。
     * ③ 如果超时了还没有获取到batchSize指定的binlog个数，也会立即返回。
     * ④ 特别的，如果没有设置超时时间，如果没有获取到binlog也立即返回。
     *
     * 4) 在CanalMetaManager中记录这个批次的binlog消息。
     * CanalMetaManager会为获取到的这个批次的binlog生成一个唯一的batchId，batchId是递增的。
     * 如果binlog信息为空，则直接把batchId设置为-1。
     *
     *
     * 不指定 position 获取事件。canal 会记住此 client 最新的 position。 <br/>
     * 如果是第一次 fetch，则会从 canal 中保存的最老一条数据开始输出。
     *
     * <pre>
     * 几种case:
     * a. 如果timeout为null，则采用tryGet方式，即时获取
     * b. 如果timeout不为null
     *    1. timeout为0，则采用get阻塞方式，获取数据，不设置超时，直到有足够的batchSize数据才返回
     *    2. timeout不为0，则采用get+timeout方式，获取数据，超时还没有batchSize足够的数据，有多少返回多少
     *
     * 注意： meta获取和数据的获取需要保证顺序性，优先拿到meta的，一定也会是优先拿到数据，所以需要加同步.
     * (不能出现先拿到meta，拿到第二批数据，这样就会导致数据顺序性出现问题)
     * </pre>
     */
    @Override
    public Message getWithoutAck(ClientIdentity clientIdentity, int batchSize, Long timeout, TimeUnit unit)
            throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        checkSubscribe(clientIdentity);

        // 1、根据destination找到要从哪一个CanalInstance中获取binlog消息
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        synchronized (canalInstance) {

            // 2、从CanalMetaManager中获取最后一个没有ack的binlog批次的位置信息。
            // 获取到流式数据中的最后一批获取的位置
            PositionRange<LogPosition> positionRanges = canalInstance.getMetaManager().getLastestBatch(clientIdentity);

            // 3、从CanalEventStore中获取binlog
            Events<Event> events = null;
            // 3.1 如果从CanalMetaManager获取到了位置信息，从当前位置继续获取binlog
            if (positionRanges != null) { // 存在流数据
                events = getEvents(canalInstance.getEventStore(), positionRanges.getStart(), batchSize, timeout, unit);
            } else {// ack后第一次获取
                //3.2 如果没有获取到binlog位置信息，从当前store中的第一条开始获取
                Position start = canalInstance.getMetaManager().getCursor(clientIdentity);
                if (start == null) { // 第一次，还没有过ack记录，则获取当前store中的第一条
                    start = canalInstance.getEventStore().getFirstPosition();
                }
                // 从CanalEventStore中获取binlog消息
                events = getEvents(canalInstance.getEventStore(), start, batchSize, timeout, unit);
            }

            // 4、记录批次信息到CanalMetaManager中
            if (CollectionUtils.isEmpty(events.getEvents())) {
                // 4.1 如果获取到的binlog消息为空，构造一个空的Message对象，将batchId设置为-1返回给客户端
                logger.debug("getWithoutAck successfully, clientId:{} batchSize:{} but result is null", new Object[]{
                        clientIdentity.getClientId(), batchSize});
                return new Message(-1, new ArrayList<Entry>()); // 返回空包，避免生成batchId，浪费性能
            } else {
                // 4.2 如果获取到了binlog消息，将这个批次的binlog消息记录到CanalMetaMaager中，并生成一个唯一的batchId
                Long batchId = canalInstance.getMetaManager().addBatch(clientIdentity, events.getPositionRange());
                // 将Events转为Entry
                List<Entry> entrys = Lists.transform(events.getEvents(), new Function<Event, Entry>() {
                    public Entry apply(Event input) {
                        return input.getEntry();
                    }
                });
                logger.info("getWithoutAck successfully, clientId:{} batchSize:{}  real size is {} and result is [batchId:{} , position:{}]",
                        clientIdentity.getClientId(),
                        batchSize,
                        entrys.size(),
                        batchId,
                        events.getPositionRange());
                return new Message(batchId, entrys);
            }

        }
    }

    /**
     * 查询当前未被ack的batch列表，batchId会按照从小到大进行返回。
     *
     */
    public List<Long> listBatchIds(ClientIdentity clientIdentity) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        checkSubscribe(clientIdentity);

        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        Map<Long, PositionRange> batchs = canalInstance.getMetaManager().listAllBatchs(clientIdentity);
        List<Long> result = new ArrayList<Long>(batchs.keySet());
        Collections.sort(result);
        return result;
    }

    /**
     * ack方法时客户端用户确认某个批次的binlog消费成功。
     *
     * 进行 batch id 的确认。确认之后，小于等于此 batchId 的 Message 都会被确认。
     * 注意：进行反馈时必须按照batchId的顺序进行ack(需有客户端保证)
     *
     * ack时需要做以下几件事情：
     *
     * 1.从CanalMetaManager中，移除这个批次的信息： （在getWithoutAck方法中，将批次的信息记录到了CanalMetaManager中，ack时移除）
     * 2.记录已经成功消费到的binlog位置，以便下一次获取的时候可以从这个位置开始，这是通过CanalMetaManager记录的。
     * 3.从CanalEventStore中，将这个批次的binlog内容移除。因为已经消费成功，继续保存这些已经消费过的binlog没有任何意义，只会白白占用内存。
     *
     */
    @Override
    public void ack(ClientIdentity clientIdentity, long batchId) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        checkSubscribe(clientIdentity);

        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        PositionRange<LogPosition> positionRanges = null;

        //1、从CanalMetaManager中，移除这个批次的信息
        positionRanges = canalInstance.getMetaManager().removeBatch(clientIdentity, batchId); // 更新位置
        if (positionRanges == null) { // 说明是重复的ack/rollback
            throw new CanalServerException(String.format("ack error , clientId:%s batchId:%d is not exist , please check",
                    clientIdentity.getClientId(),
                    batchId));
        }

        // 更新cursor最好严格判断下位置是否有跳跃更新
        // Position position = lastRollbackPostions.get(clientIdentity);
        // if (position != null) {
        // // Position position =
        // canalInstance.getMetaManager().getCursor(clientIdentity);
        // LogPosition minPosition =
        // CanalEventUtils.min(positionRanges.getStart(), (LogPosition)
        // position);
        // if (minPosition == position) {// ack的position要晚于该最后ack的位置，可能有丢数据
        // throw new CanalServerException(
        // String.format(
        // "ack error , clientId:%s batchId:%d %s is jump ack , last ack:%s",
        // clientIdentity.getClientId(), batchId, positionRanges,
        // position));
        // }
        // }

        // 2、记录已经成功消费到的binlog位置，以便下一次获取的时候可以从这个位置开始，这是通过CanalMetaManager记录的
        if (positionRanges.getAck() != null) {
            canalInstance.getMetaManager().updateCursor(clientIdentity, positionRanges.getAck());
            logger.info("ack successfully, clientId:{} batchId:{} position:{}",
                    clientIdentity.getClientId(),
                    batchId,
                    positionRanges);
        }

        // 3、从CanalEventStore中，将这个批次的binlog内容移除
        canalInstance.getEventStore().ack(positionRanges.getEnd());
    }

    /**
     * 回滚到未进行 {@link #ack} 的地方，下次fetch的时候，可以从最后一个没有 {@link #ack} 的地方开始拿
     */
    @Override
    public void rollback(ClientIdentity clientIdentity) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());

        // 因为存在第一次链接时自动rollback的情况，所以需要忽略未订阅
        boolean hasSubscribe = canalInstance.getMetaManager().hasSubscribe(clientIdentity);
        if (!hasSubscribe) {
            return;
        }

        synchronized (canalInstance) {
            // 清除batch信息
            canalInstance.getMetaManager().clearAllBatchs(clientIdentity);
            // rollback eventStore中的状态信息
            canalInstance.getEventStore().rollback();
            logger.info("rollback successfully, clientId:{}", new Object[]{clientIdentity.getClientId()});
        }
    }

    /**
     * 回滚到未进行 {@link #ack} 的地方，下次fetch的时候，可以从最后一个没有 {@link #ack} 的地方开始拿
     *
     */
    @Override
    public void rollback(ClientIdentity clientIdentity, Long batchId) throws CanalServerException {
        checkStart(clientIdentity.getDestination());
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());

        // 因为存在第一次链接时自动rollback的情况，所以需要忽略未订阅
        boolean hasSubscribe = canalInstance.getMetaManager().hasSubscribe(clientIdentity);
        if (!hasSubscribe) {
            return;
        }

        synchronized (canalInstance) {
            // 清除batch信息
            PositionRange<LogPosition> positionRanges = canalInstance.getMetaManager().removeBatch(clientIdentity,batchId);
            if (positionRanges == null) { // 说明是重复的ack/rollback
                throw new CanalServerException(String.format("rollback error, clientId:%s batchId:%d is not exist , please check",
                        clientIdentity.getClientId(),
                        batchId));
            }

            // lastRollbackPostions.put(clientIdentity,
            // positionRanges.getEnd());// 记录一下最后rollback的位置
            // TODO 后续rollback到指定的batchId位置
            canalInstance.getEventStore().rollback();// rollback eventStore中的状态信息
            logger.info("rollback successfully, clientId:{} batchId:{} position:{}",
                    clientIdentity.getClientId(),
                    batchId,
                    positionRanges);
        }
    }

    public Map<String, CanalInstance> getCanalInstances() {
        return Maps.newHashMap(canalInstances);
    }

    // ======================== helper method =======================

    /**
     * 根据不同的参数，选择不同的方式获取数据
     * 1） 如果timeout为null，则采用tryGet方式，即时获取
     * 2）如果timeout不为null
     * -- 1. timeout为0，则采用get阻塞方式，获取数据，不设置超时，直到有足够的batchSize数据才返回
     * -- 2. timeout不为0，则采用get+timeout方式，获取数据，超时还没有batchSize足够的数据，有多少返回多少
     *
     */
    private Events<Event> getEvents(CanalEventStore eventStore, Position start, int batchSize, Long timeout, TimeUnit unit) {

        // 如果timeout为null，则采用tryGet方式，即时获取
        if (timeout == null) {
            return eventStore.tryGet(start, batchSize);
        } else {

            try {
                // 如果timeout为0，则采用get阻塞方式，获取数据，不设置超时，直到有足够的batchSize数据才返回
                if (timeout <= 0) {
                    return eventStore.get(start, batchSize);
                } else {
                    // timeout不为0，则采用get+timeout方式，获取数据，超时还没有batchSize足够的数据，有多少返回多少
                    return eventStore.get(start, batchSize, timeout, unit);
                }
            } catch (Exception e) {
                throw new CanalServerException(e);
            }
        }
    }

    private void checkSubscribe(ClientIdentity clientIdentity) {
        CanalInstance canalInstance = canalInstances.get(clientIdentity.getDestination());
        boolean hasSubscribe = canalInstance.getMetaManager().hasSubscribe(clientIdentity);
        if (!hasSubscribe) {
            throw new CanalServerException(String.format("ClientIdentity:%s should subscribe first",
                    clientIdentity.toString()));
        }
    }

    private void checkStart(String destination) {
        if (!isStart(destination)) {
            throw new CanalServerException(String.format("destination:%s should start first", destination));
        }
    }

    // ========= setter ==========

    public void setCanalInstanceGenerator(CanalInstanceGenerator canalInstanceGenerator) {
        this.canalInstanceGenerator = canalInstanceGenerator;
    }

}
