package com.alibaba.otter.canal.store;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.store.model.Events;

/**
 * canel数据存储接口
 * 
 * @author jianghang 2012-6-14 下午08:44:52
 * @version 1.0.0
 */
public interface CanalEventStore<T> extends CanalLifeCycle, CanalStoreScavenge {

    // ==========================Put操作==============================
    // Put操作是canal parser模块解析binlog事件，并经过sink模块过滤后，放入到store模块中，也就是说Put操作实际上是canal内部调用。
    /**
     * 添加一组数据对象，阻塞等待其操作完成 (比如一次性添加一个事务数据)
     */
    void put(List<T> data) throws InterruptedException, CanalStoreException;

    /**
     * 添加一组数据对象，阻塞等待其操作完成或者时间超时 (比如一次性添加一个事务数据)
     */
    boolean put(List<T> data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException;

    /**
     * 添加一组数据对象 (比如一次性添加一个事务数据)
     */
    boolean tryPut(List<T> data) throws CanalStoreException;

    /**
     * 添加一个数据对象，阻塞等待其操作完成
     */
    void put(T data) throws InterruptedException, CanalStoreException;

    /**
     * 添加一个数据对象，阻塞等待其操作完成或者时间超时
     */
    boolean put(T data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException;

    /**
     * 添加一个数据对象
     */
    boolean tryPut(T data) throws CanalStoreException;

    // ==========================GET操作==============================
    // Get操作(以及ack、rollback)则不同，其是由client发起的网络请求，server端通过对请求参数进行解析，最终调用CanalEventStore模块中定义的对应方法。
    /**
     * 获取指定大小的数据，阻塞等待其操作完成
     */
    Events<T> get(Position start, int batchSize) throws InterruptedException, CanalStoreException;

    /**
     * 获取指定大小的数据，阻塞等待其操作完成或者超时，如果超时了，有多少，返回多少
     */
    Events<T> get(Position start, int batchSize, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException;

    /**
     * 尝试获取，如果获取不到立即返回
     */
    Events<T> tryGet(Position start, int batchSize) throws CanalStoreException;


    // =========================获取位置的操作==============================
    /**
     * 获取最后一条数据的position
     */
    Position getLatestPosition() throws CanalStoreException;

    /**
     * 获取第一条数据的position，如果没有数据返回为null
     */
    Position getFirstPosition() throws CanalStoreException;


    // =========================Ack操作==============================
    /**
     * 删除{@linkplain Position}之前的数据
     */
    void ack(Position position) throws CanalStoreException;

    // =========================回滚操作==============================
    /**
     * 出错时执行回滚操作(未提交ack的所有状态信息重新归位，减少出错时数据全部重来的成本)
     */
    void rollback() throws CanalStoreException;

}
