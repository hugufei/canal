package com.alibaba.otter.canal.store.memory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.alibaba.otter.canal.store.AbstractCanalStoreScavenge;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.CanalStoreException;
import com.alibaba.otter.canal.store.CanalStoreScavenge;
import com.alibaba.otter.canal.store.helper.CanalEventUtils;
import com.alibaba.otter.canal.store.model.BatchMode;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;

/**
 * 基于内存buffer构建内存memory store
 * PS: MemoryEventStoreWithBuffer的实现借鉴了Disruptor的RingBuffer
 *
 * <p>
 * 变更记录：
 * 1. 新增BatchMode类型，支持按内存大小获取批次数据，内存大小更加可控.
 * a. put操作，会首先根据bufferSize进行控制，然后再进行bufferSize * bufferMemUnit进行控制. 因存储的内容是以Event，如果纯依赖于memsize进行控制，会导致RingBuffer出现动态伸缩
 *
 * @author jianghang 2012-6-20 上午09:46:31
 * @version 1.0.0
 */

// MemoryEventStoreWithBuffer是目前开源版本中的CanalEventStore接口的唯一实现，基于内存模式。
// 当然你也可以进行扩展，提供一个基于本地文件存储方式的CanalEventStore实现。这样就可以一份数据让多个业务费进行订阅，只要独立维护消费位置元数据即可。
// 然而，我不得不提醒你的是，基于本地文件的存储方式，一定要考虑好数据清理工作，否则会有大坑。

// 如果一个库只有一个业务方订阅，其实根本也不用实现本地存储，使用基于内存模式的队列进行缓存即可。
// 如果client消费的快，那么队列中的数据放入后就被取走，队列基本上一直是空的，实现本地存储也没意义；
// 如果client消费的慢，队列基本上一直是满的，只要client来获取，总是能拿到数据，因此也没有必要实现本地存储。
public class MemoryEventStoreWithBuffer extends AbstractCanalStoreScavenge implements CanalEventStore<Event>, CanalStoreScavenge {

    private static final long INIT_SQEUENCE = -1;
    private int bufferSize = 16 * 1024;
    private int bufferMemUnit = 1024; // memsize的单位，默认为1kb大小

    // 用于对putSequence、getSequence、ackSequence进行取余操作。
    // 前面已经介绍过canal通过位操作进行取余，其值为bufferSize-1
    private int indexMask;

    //数组类型，环形队列底层基于的Event[]数组，队列的大小就是bufferSize。
    private Event[] entries;

    // 记录下put/get/ack操作的三个下标,初始值都是-1
    // 每放入一个数据putSequence +1，可表示存储数据存储的总数量
    private AtomicLong putSequence = new AtomicLong(INIT_SQEUENCE); // 代表当前put操作最后一次写操作发生的位置
    // 每获取一个数据getSequence +1，可表示数据订阅获取的最后一次提取位置
    private AtomicLong getSequence = new AtomicLong(INIT_SQEUENCE); // 代表当前get操作读取的最后一条的位置
    // 每确认一个数据ackSequence + 1，可表示数据最后一次消费成功位置
    private AtomicLong ackSequence = new AtomicLong(INIT_SQEUENCE); // 代表当前ack操作的最后一条的位置
    // putSequence、getSequence、ackSequence这3个变量初始值都是-1，且都是递增的，均用long型表示。
    // 由于数据只有被Put进来后，才能进行Get；Get之后才能进行Ack。 所以，这三个变量满足以下关系：
    // ackSequence <= getSequence <= putSequence
    // 计算当前可消费的event数量：putSequence - getSequence
    // 计算当前队列的大小(即队列中还有多少事件等待消费)：putSequence - ackSequence


    // 记录下put/get/ack操作的三个memsize大小
    // 分别用于记录put/get/ack操作的event占用内存的累加值，都是从0开始计算。
    // 例如每put一个event，putMemSize就要增加这个event占用的内存大小；get和ack操作也是类似。
    // 这三个变量，都是在batchMode指定为MEMSIZE的情况下，才会发生作用。
    // 1) 计算出当前环形队列当前占用的内存大小 :  putMemSize - ackMemSize
    // 2) 计算尚未被获取的事件占用的内存大小 :  putMemSize - getMemSize
    private AtomicLong putMemSize = new AtomicLong(0);
    private AtomicLong getMemSize = new AtomicLong(0);
    private AtomicLong ackMemSize = new AtomicLong(0);

    // 阻塞put/get操作控制信号
    private ReentrantLock lock = new ReentrantLock();
    private Condition notFull = lock.newCondition();
    private Condition notEmpty = lock.newCondition();

    // 默认为内存大小模式
    // batchMode除了对PUT操作有限制，对Get操作也有影响。
    // 1) Get操作可以指定一个batchSize，用于指定批量获取的大小。
    // 2) 当batchMode为MEMSIZE时，其含义就在不再是记录数，而是要获取到总共占用 batchSize * bufferMemUnit 内存大小的事件数量。
    private BatchMode batchMode = BatchMode.ITEMSIZE;
    private boolean ddlIsolation = false;

    public MemoryEventStoreWithBuffer() {

    }

    public MemoryEventStoreWithBuffer(BatchMode batchMode) {
        this.batchMode = batchMode;
    }

    // 初始化MemoryEventStoreWithBuffer内部的环形队列，其实就是初始化一下Event[]数组。
    public void start() throws CanalStoreException {
        super.start();
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }
        // 初始化indexMask，前面已经介绍过，用于通过位操作进行取余
        indexMask = bufferSize - 1;
        // 创建循环队列基于的底层数组，大小为bufferSize
        entries = new Event[bufferSize];
    }

    // 停止，在停止时会清空所有缓存的数据，将维护的相关状态变量设置为初始值。
    public void stop() throws CanalStoreException {
        super.stop();
        // 清空所有缓存的数据，将维护的相关状态变量设置为初始值
        cleanAll();
    }

    // Put 操作：添加数据。event parser模块拉取到binlog后，并经过event sink模块过滤，最终就通过Put操作存储到了队列中。
    public void put(List<Event> data) throws InterruptedException, CanalStoreException {
        //1: 如果需要插入的List为空，直接返回true
        if (data == null || data.isEmpty()) {
            return;
        }
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                while (!checkFreeSlotAt(putSequence.get() + data.size())) { // 检查是否有空位
                    notFull.await(); // wait until not full
                }
            } catch (InterruptedException ie) {
                notFull.signal(); // propagate to non-interrupted thread
                throw ie;
            }
            doPut(data);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        } finally {
            lock.unlock();
        }
    }

    // 可以指定timeout超时时间的put方法
    // Put操作是canal parser模块解析binlog事件，并经过sink模块过滤后，放入到store模块中，也就是说Put操作实际上是canal内部调用。
    public boolean put(List<Event> data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        // 1 如果需要插入的List为空，直接返回true
        if (data == null || data.isEmpty()) {
            return true;
        }
        // 2 获得超时时间，并通过加锁进行put操作
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            //这是一个死循环，执行到下面任意一个return或者抛出异常是时才会停止
            for (;;) {
                // 3 检查是否满足插入条件，如果满足，进入到3.1，否则进入到3.2
                if (checkFreeSlotAt(putSequence.get() + data.size())) {
                    // 3.1 如果满足条件，调用doPut方法进行真正的插入
                    doPut(data);
                    return true;
                }
                // 3.2 判断是否已经超时，如果超时，则不执行插入操作，直接返回false
                if (nanos <= 0) {
                    return false;
                }
                // 3.3 如果还没有超时，调用notFull.awaitNanos进行等待，需要其他线程调用notFull.signal()方法唤醒。
                // 唤醒是在ack操作中进行的，ack操作会删除已经消费成功的event，此时队列有了空间，因此可以唤醒，详见ack方法分析
                // 当被唤醒后，因为这是一个死循环，所以循环中的代码会重复执行。当插入条件满足时，调用doPut方法插入，然后返回
                try {
                    // 3.4 如果一直等待到超时，都没有可用空间可以插入，notFull.awaitNanos会抛出InterruptedException
                    nanos = notFull.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    // 3.5 超时之后，唤醒一个其他执行put操作且未被中断的线程(不明白是为了干啥)
                    notFull.signal(); // propagate to non-interrupted thread
                    throw ie;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean tryPut(List<Event> data) throws CanalStoreException {
        if (data == null || data.isEmpty()) {
            return true;
        }

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (!checkFreeSlotAt(putSequence.get() + data.size())) {
                return false;
            } else {
                doPut(data);
                return true;
            }
        } finally {
            lock.unlock();
        }
    }

    public void put(Event data) throws InterruptedException, CanalStoreException {
        put(Arrays.asList(data));
    }

    public boolean put(Event data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        return put(Arrays.asList(data), timeout, unit);
    }

    public boolean tryPut(Event data) throws CanalStoreException {
        return tryPut(Arrays.asList(data));
    }

    /**
     * 执行具体的put操作
     * 1、将新插入的event数据赋值到Event[]数组的正确位置上，就算完成了插入
     * 2、当新插入的event记录数累加到putSequence上
     * 3、累加新插入的event的大小到putMemSize上
     * 4、调用notEmpty.signal()方法，通知队列中有数据了，如果之前有client获取数据处于阻塞状态，将会被唤醒
     */
    private void doPut(List<Event> data) {
        //1 将新插入的event数据赋值到Event[]数组的正确位置上

        //1.1 获得putSequence的当前值current，和插入数据后的putSequence结束值end
        long current = putSequence.get();
        long end = current + data.size();

        //1.2 循环需要插入的数据，从current位置开始，到end位置结束
        for (long next = current + 1; next <= end; next++) {
            //1.3 通过getIndex方法对next变量转换成正确的位置，设置到Event[]数组中
            //需要转换的原因在于，这里的Event[]数组是环形队列的底层实现，其大小为bufferSize值，默认为16384。
            //运行一段时间后，接收到的binlog数量肯定会超过16384，每接受到一个event，putSequence+1，因此最终必然超过这个值。
            //而next变量是比当前putSequence值要大的，因此必须进行转换，否则会数组越界，转换工作就是在getIndex方法中进行的
            entries[getIndex(next)] = data.get((int) (next - current - 1));
        }
        // 2 直接设置putSequence为end值，相当于完成event记录数的累加
        putSequence.set(end);

        // 3 累加新插入的event的大小到putMemSize上
        if (batchMode.isMemSize()) {
            // 用于记录本次插入的event记录的大小
            long size = 0;
            for (Event event : data) {
                size += calculateSize(event);
            }
            // 将size变量的值，添加到当前putMemSize
            putMemSize.getAndAdd(size);
        }

        // 4 调用notEmpty.signal()方法，通知队列中有数据了，如果之前有client获取数据处于阻塞状态，将会被唤醒
        notEmpty.signal();
    }

    // 获取数据。canal client连接到canal server后，最终获取到的binlog都是从这个队列中取得。
    // Get操作(以及ack、rollback)则不同，其是由client发起的网络请求，server端通过对请求参数进行解析，最终调用CanalEventStore模块中定义的对应方法。
    public Events<Event> get(Position start, int batchSize) throws InterruptedException, CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                while (!checkUnGetSlotAt((LogPosition) start, batchSize))
                    notEmpty.await();
            } catch (InterruptedException ie) {
                notEmpty.signal(); // propagate to non-interrupted thread
                throw ie;
            }
            return doGet(start, batchSize);
        } finally {
            lock.unlock();
        }
    }

    // 获取指定大小的数据，阻塞等待其操作完成或者超时，如果超时了，有多少，返回多少
    public Events<Event> get(Position start, int batchSize, long timeout, TimeUnit unit) throws InterruptedException,CanalStoreException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (;;) {
                // 检查是否有足够的event可供获取
                if (checkUnGetSlotAt((LogPosition) start, batchSize)) {
                    return doGet(start, batchSize);
                }
                if (nanos <= 0) {
                    // 如果时间到了，有多少取多少
                    return doGet(start, batchSize);
                }
                try {
                    nanos = notEmpty.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    notEmpty.signal(); // propagate to non-interrupted thread
                    throw ie;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public Events<Event> tryGet(Position start, int batchSize) throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return doGet(start, batchSize);
        } finally {
            lock.unlock();
        }
    }

    // doGet方法会进行真正的数据获取操作，获取主要分为5个步骤：
    // 1、确定从哪个位置开始获取数据
    // 2、根据batchMode是MEMSIZE还是ITEMSIZE，通过不同的方式来获取数据
    // 3、设置PositionRange，表示获取到的event列表开始和结束位置
    // 4、设置ack点
    // 5、累加getSequence，getMemSize值
    private Events<Event> doGet(Position start, int batchSize) throws CanalStoreException {
        LogPosition startPosition = (LogPosition) start;

        // 1 确定从哪个位置开始获取数据
        // 获得当前的get位置
        long current = getSequence.get();
        // 获得当前的put位置
        long maxAbleSequence = putSequence.get();
        // 要获取的第一个Event的位置，一开始等于当前get位置
        long next = current;

        // 要获取的最后一个event的位置，一开始也是当前get位置，每获取一个event，end值加1，最大为current+batchSize
        // 因为可能进行ddl隔离，因此可能没有获取到batchSize个event就返回了，此时end值就会小于current+batchSize
        long end = current;

        // 如果startPosition为null，说明是第一次订阅，默认+1处理，因为getSequence的值是从-1开始的
        // 如果tartPosition不为null，需要包含一下start位置，防止丢失第一条记录
        if (startPosition == null || !startPosition.getPostion().isIncluded()) {
            next = next + 1;
        }

        // 如果没有数据，直接返回一个空列表
        if (current >= maxAbleSequence) {
            return new Events<Event>();
        }

        // 2 如果有数据，根据batchMode是ITEMSIZE或MEMSIZE选择不同的处理方式
        Events<Event> result = new Events<Event>();
        // 维护要返回的Event列表
        List<Event> entrys = result.getEvents();
        long memsize = 0;
        if (batchMode.isItemSize()) {
            end = (next + batchSize - 1) < maxAbleSequence ? (next + batchSize - 1) : maxAbleSequence;
            // 2.1.1 循环从开始位置(next)到结束位置(end)，每次循环next+1
            for (; next <= end; next++) {
                Event event = entries[getIndex(next)];
                // 2.1.3 果是当前事件是DDL事件，且开启了ddl隔离，本次事件处理完后，即结束循环(if语句最后是一行是break)
                // 1) 数据查询语言DQL: SELECT子句，FROM子句，WHERE
                // 2) 数据操纵语言DML: INSERT|UPDATE|DELETE
                // 3) 数据定义语言DDL: CREATE TABLE/VIEW/INDEX/SYN/CLUSTER
                // 4) 数据控制语言DCL: GRANT| ROLLBACK |COMMIT
                if (ddlIsolation && isDdl(event.getEntry().getHeader().getEventType())) {
                    // 2.1.4 因为ddl事件需要单独返回，因此需要判断entrys中是否应添加了其他事件
                    if (entrys.size() == 0) { // 如果entrys中尚未添加任何其他event
                        entrys.add(event);// 加入当前的DDL事件
                        end = next; // 更新end为当前
                    } else {
                        // 如果已经添加了其他事件 如果之前已经有DML事件，直接返回了，因为不包含当前next这记录，需要回退一个位置
                        end = next - 1; // next-1一定大于current，不需要判断
                    }
                    break;
                } else {
                    // 如果没有开启DDL隔离，直接将事件加入到entrys中
                    entrys.add(event);
                }
            }
        } else {
            //2.2 如果batchMode是MEMSIZE
            // 2.2.1 计算本次要获取的event占用最大字节数
            long maxMemSize = batchSize * bufferMemUnit;
            // 2.2.2 memsize从0开始，当memsize小于maxMemSize且next未超过maxAbleSequence时，可以进行循环
            for (; memsize <= maxMemSize && next <= maxAbleSequence; next++) {
                // 2.2.3 获取指定位置上的Event [永远保证可以取出第一条的记录，避免死锁??]
                Event event = entries[getIndex(next)];
                // 2.2.4 果是当前事件是DDL事件，且开启了ddl隔离，本次事件处理完后，即结束循环(if语句最后是一行是break)
                if (ddlIsolation && isDdl(event.getEntry().getHeader().getEventType())) {
                    // 如果是ddl隔离，直接返回
                    if (entrys.size() == 0) {
                        entrys.add(event);// 如果没有DML事件，加入当前的DDL事件
                        end = next; // 更新end为当前
                    } else {
                        // 如果之前已经有DML事件，直接返回了，因为不包含当前next这记录，需要回退一个位置
                        end = next - 1; // next-1一定大于current，不需要判断
                    }
                    break;
                } else {
                    // 如果没有开启DDL隔离，直接将事件加入到entrys中
                    entrys.add(event);
                    // 并将当前添加的event占用字节数累加到memsize变量上
                    memsize += calculateSize(event);
                    end = next;// 记录end位点
                }
            }
        }

        // 3 构造PositionRange，表示本次获取的Event的开始和结束位置
        // 事实上，parser模块解析后，已经将位置信息：binlog文件，position封装到了Event中，createPosition方法只是将这些信息提取出来。
        PositionRange<LogPosition> range = new PositionRange<LogPosition>();
        result.setPositionRange(range);
        // 3.1 把entrys列表中的第一个event的位置，当做PositionRange的开始位置
        range.setStart(CanalEventUtils.createPosition(entrys.get(0)));
        // 3.2 把entrys列表中的最后一个event的位置，当做PositionRange的结束位置
        range.setEnd(CanalEventUtils.createPosition(entrys.get(result.getEvents().size() - 1)));

        // 4 记录一下是否存在可以被ack的点，逆序迭代获取到的Event列表
        // PS:
        // mysql原生的binlog事件中，总是以一个内容”BEGIN”的QueryEvent作为事务开始，以XidEvent事件表示事务结束。
        // 即使我们没有显式的开启事务，对于单独的一个更新语句(如Insert、update、delete)，mysql也会默认开启事务。
        // 而canal将其转换成更容易理解的自定义EventType类型：TRANSACTIONBEGIN、TRANSACTIONEND。
        // 而将这些事件作为ack点，主要是为了保证事务的完整性。例如:
        // 1) client一次拉取了10个binlog event，前5个构成一个事务，后5个还不足以构成一个完整事务。
        // 2) 在ack后，如果这个client停止了，也就是说下一个事务还没有被完整处理完。
        // 3) 尽管之前ack的是10条数据，但是client重新启动后，将从第6个event开始消费，而不是从第11个event开始消费，因为第6个event是下一个事务的开始。

        // 具体逻辑在于，canal server在接受到client ack后，CanalServerWithEmbedded#ack方法会执行。
        // 其内部首先根据ack的batchId找到对应的PositionRange，再找出其中的ack点，通过CanalMetaManager将这个位置记录下来。
        // 之后client重启后，再把这个位置信息取出来，从这个位置开始消费。
        // 也就是说，ack位置实际上提供给CanalMetaManager使用的。
        // 而对于MemoryEventStoreWithBuffer本身而言，也需要进行ack，用于将已经消费的数据从队列中清除，从而腾出更多的空间存放新的数据。
        for (int i = entrys.size() - 1; i >= 0; i--) {
            Event event = entrys.get(i);
            // 4.1.1 如果是事务开始/事务结束/或者dll事件
            if (CanalEntry.EntryType.TRANSACTIONBEGIN == event.getEntry().getEntryType()
                    || CanalEntry.EntryType.TRANSACTIONEND == event.getEntry().getEntryType()
                    || isDdl(event.getEntry().getHeader().getEventType())) {
                //  4.1.2 将事务头/尾设置可被为ack的点,并跳出循环
                range.setAck(CanalEventUtils.createPosition(event));
                break;
            }
            // 4.1.3 如果没有这三种类型事件，意味着没有可被ack的点
        }

        //5 累加getMemSize值，getMemSize值
        //5.1 通过AtomLong的compareAndSet尝试增加getSequence值
        if (getSequence.compareAndSet(current, end)) {
            getMemSize.addAndGet(memsize);
            // 如果之前有put操作因为队列满了而被阻塞，这里发送信号，通知队列已经有空位置
            notFull.signal();
            return result;
        } else {
            // 如果失败，直接返回空事件列表
            return new Events<Event>();
        }
    }

    // 获//获取第一条数据的position，如果没有数据返回为null
    public LogPosition getFirstPosition() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long firstSeqeuence = ackSequence.get();
            // 1 没有ack过数据，且队列中有数据
            if (firstSeqeuence == INIT_SQEUENCE && firstSeqeuence < putSequence.get()) {
                // 没有ack过数据，那么ack为初始值-1，又因为队列中有数据，因此ack+1,即返回队列中第一条数据的位置
                Event event = entries[getIndex(firstSeqeuence + 1)]; // 最后一次ack为-1，需要移动到下一条,included = false
                return CanalEventUtils.createPosition(event, false);
            } else if (firstSeqeuence > INIT_SQEUENCE && firstSeqeuence < putSequence.get()) {
                // 2 已经ack过数据，但是未追上put操作
                // 返回最后一次ack的位置数据 + 1
                Event event = entries[getIndex(firstSeqeuence + 1)]; // 最后一次ack的位置数据+ 1
                return CanalEventUtils.createPosition(event, true);
            } else if (firstSeqeuence > INIT_SQEUENCE && firstSeqeuence == putSequence.get()) {
                // 3 已经ack过数据，且已经追上put操作，说明队列中所有数据都被消费完了
                // 最后一次ack的位置数据，和last为同一条
                Event event = entries[getIndex(firstSeqeuence)]; // 最后一次ack的位置数据，和last为同一条，included = false
                return CanalEventUtils.createPosition(event, false);
            } else {
                // 没有任何数据
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    // 获取当前队列中最后一个Event的位置信息
    public LogPosition getLatestPosition() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long latestSequence = putSequence.get();
            if (latestSequence > INIT_SQEUENCE && latestSequence != ackSequence.get()) {
                Event event = entries[(int) putSequence.get() & indexMask]; // 最后一次写入的数据，最后一条未消费的数据
                return CanalEventUtils.createPosition(event, true);
            } else if (latestSequence > INIT_SQEUENCE && latestSequence == ackSequence.get()) {
                // ack已经追上了put操作
                Event event = entries[(int) putSequence.get() & indexMask]; // 最后一次写入的数据，included
                // =
                // false
                return CanalEventUtils.createPosition(event, false);
            } else {
                // 没有任何数据
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    // 确认消费成功。canal client获取到binlog事件消费后，需要进行Ack。
    // 你可以认为Ack操作实际上就是将消费成功的事件从队列中删除。
    // 因为如果一直不Ack的话，队列满了之后，Put操作就无法添加新的数据了。
    public void ack(Position position) throws CanalStoreException {
        cleanUntil(position);
    }

    // postion表示要ack的配置
    public void cleanUntil(Position position) throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            // 获得当前ack值
            long sequence = ackSequence.get();
            // 获得当前get值
            long maxSequence = getSequence.get();

            boolean hasMatch = false;
            long memsize = 0;

            // 迭代所有未被ack的event，从中找出与需要ack的position相同位置的event，清空这个event之前的所有数据。
            // 一旦找到这个event，循环结束。
            for (long next = sequence + 1; next <= maxSequence; next++) {
                // 获得要ack的event
                Event event = entries[getIndex(next)];
                // 计算当前要ack的event占用字节数
                memsize += calculateSize(event);
                // 检查是否匹配上
                boolean match = CanalEventUtils.checkPosition(event, (LogPosition) position);
                // 找到对应的position，更新ack seq
                if (match) {
                    hasMatch = true;
                    // 如果batchMode是MEMSIZE
                    if (batchMode.isMemSize()) {
                        // 累加ackMemSize
                        ackMemSize.addAndGet(memsize);
                        // 尝试清空buffer中的内存，将ack之前的内存全部释放掉
                        for (long index = sequence + 1; index < next; index++) {
                            entries[getIndex(index)] = null;// 设置为null
                        }
                    }
                    // 避免并发ack
                    // 官方注释说，采用compareAndSet，是为了避免并发ack。我觉得根本不会并发ack，因为都加锁了
                    if (ackSequence.compareAndSet(sequence, next)) {
                        notFull.signal();
                        return;
                    }
                }
            }

            if (!hasMatch) {// 找不到对应需要ack的position
                throw new CanalStoreException("no match ack position" + position.toString());
            }
        } finally {
            lock.unlock();
        }
    }

    // 相对于put/get/ack操作，rollback操作简单了很多。
    // 所谓rollback，就是client已经get到的数据，没能消费成功，因此需要进行回滚。
    // 回滚操作特别简单，只需要将getSequence的位置重置为ackSequence，将getMemSize设置为ackMemSize即可。
    public void rollback() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            getSequence.set(ackSequence.get());
            getMemSize.set(ackMemSize.get());
        } finally {
            lock.unlock();
        }
    }

    public void cleanAll() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            // 将Put/Get/Ack三个操作的位置都重置为初始状态-1
            putSequence.set(INIT_SQEUENCE);
            getSequence.set(INIT_SQEUENCE);
            ackSequence.set(INIT_SQEUENCE);

            // 将Put/Get/Ack三个操作的memSize都重置为0
            putMemSize.set(0);
            getMemSize.set(0);
            ackMemSize.set(0);

            // 将底层Event[]数组置为null，相当于清空所有数据
            entries = null;
            // for (int i = 0; i < entries.length; i++) {
            // entries[i] = null;
            // }
        } finally {
            lock.unlock();
        }
    }

    // =================== helper method =================
    // 用于返回getSequence和ackSequence二者的较小值
    private long getMinimumGetOrAck() {
        long get = getSequence.get();
        long ack = ackSequence.get();
        return ack <= get ? ack : get;
    }

    /**
     * 执行插入数据前的检查工作:
     * 1) 检查是否足够的slot:
     * 默认的bufferSize设置大小为16384，即有16384个slot，每个slot可以存储一个event，因此canal默认最多缓存16384个event。
     * 从来另一个角度出发，这意味着putSequence最多比ackSequence可以大16384，不能超过这个值。
     * 如果超过了，就意味着尚未没有被消费的数据被覆盖了，相当于丢失了数据。
     * 因此，如果Put操作满足以下条件时，是不能新加入数据的:
     * (putSequence + need_put_events_size)- ackSequence > bufferSize
     *
     *
     * 2) 检测是否超出了内存限制:
     * 前面我们已经看到了，为了控制队列中event占用的总内存大小，可以指定batchMode为MEMSIZE。
     * 在这种情况下，buffer.size  * buffer.memunit(默认为16M)就表示环形队列存储的event总共可以占用的内存大小。
     * 因此当出现以下情况下， 不能加入新的event：
     * (putMemSize - ackMemSize) > buffer.size  * buffer.memunit
     *  putMemSize和ackMemSize的差值，其实就是 队列当前包含的event占用的总内存
     */
    private boolean checkFreeSlotAt(final long sequence) {
        // 1、检查是否足够的slot。注意方法参数传入的sequence值是：当前putSequence值 + 新插入的event的记录数。
        //按照前面的说明，其减去bufferSize不能大于ack位置，或者换一种说法，减去bufferSize不能大于ack位置。
        // 1.1 首先用sequence值减去bufferSize
        final long wrapPoint = sequence - bufferSize;
        // 1.2 获取get位置ack位置的较小值，事实上，ack位置总是应该小于等于get位置，因此这里总是应该返回的是ack位置。
        final long minPoint = getMinimumGetOrAck();
        // 1.3 将1.1 与1.2步得到的值进行比较，如果前者大，说明二者差值已经超过了bufferSize，不能插入数据，返回false
        if (wrapPoint > minPoint) { // 刚好追上一轮
            return false;
        } else {
            //2 如果batchMode是MEMSIZE，继续检查是否超出了内存限制。
            if (batchMode.isMemSize()) {
                // 2.1 使用putMemSize值减去ackMemSize值，得到当前保存的event事件占用的总内存
                final long memsize = putMemSize.get() - ackMemSize.get();
                // 2.2 如果没有超出bufferSize * bufferMemUnit内存限制，返回true，否则返回false
                if (memsize < bufferSize * bufferMemUnit) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        }
    }

    /**
     * 检查是否存在需要get的数据,并且数量>=batchSize
     */
    private boolean checkUnGetSlotAt(LogPosition startPosition, int batchSize) {
        // 如果batchMode为ITEMSIZE，则表示只要有有满足batchSize数量的记录数即可，即putSequence - getSequence >= batchSize
        if (batchMode.isItemSize()) {
            long current = getSequence.get();
            long maxAbleSequence = putSequence.get();
            long next = current;
            // 1.1 第一次订阅之后，需要包含一下start位置，防止丢失第一条记录。

            // 首先要明确checkUnGetSlotAt方法的startPosition参数到底是从哪里传递过来的:
            // 当一个client在获取数据时，CanalServerWithEmbedded的getWithoutAck/或get方法会被调用。
            // 其内部首先通过CanalMetaManager查找client的消费位置信息，由于是第一次，肯定没有记录，因此返回null，
            // 此时会调用CanalEventStore的getFirstPosition()方法，尝试把第一条数据作为消费的开始。
            // 而此时CanalEventStore中可能有数据，也可能没有数据。
            // 在没有数据的情况下，依然返回null；
            // 在有数据的情况下，把第一个Event的位置作为消费开始位置。
            // 那么显然，传入checkUnGetSlotAt方法的startPosition参数可能是null，也可能不是null。所以有了以下处理逻辑：
            if (startPosition == null || !startPosition.getPostion().isIncluded()) {
                // 如果不是null的情况下，尽管把第一个event当做开始位置，但是因为这个event毕竟还没有消费，所以在消费的时候我们必须也将其包含进去。之所以要+1
                // 因为是第一次获取，getSequence的值肯定还是初始值-1，所以要+1变成0之后才是队列的第一个event位置。
                next = next + 1;// 少一条数据
            }
            // 1.2 理论上只需要满足条件：putSequence - getSequence >= batchSize
            // 1.2.1 先通过current < maxAbleSequence进行一下简单判断，如果不满足，可以直接返回false了
            // 1.2.2 如果1.2.1满足，再通过putSequence - getSequence >= batchSize判断是否有足够的数据
            if (current < maxAbleSequence && next + batchSize - 1 <= maxAbleSequence) {
                return true;
            } else {
                return false;
            }
        } else {
            // 2 如果batchMode为MEMSIZE
            // 处理内存大小判断
            long currentSize = getMemSize.get();
            long maxAbleSize = putMemSize.get();
            //2.1 需要满足条件 putMemSize-getMemSize >= batchSize * bufferMemUnit
            if (maxAbleSize - currentSize >= batchSize * bufferMemUnit) {
                return true;
            } else {
                return false;
            }
        }
    }

    // 对于batchMode是MEMSIZE的情况下， 还会通过calculateSize方法计算每个event占用的内存大小，累加到putMemSize上。
    // 其原理在于，mysql的binlog的event header中，都有一个event_length表示这个event占用的字节数。
    // parser模块将二进制形式binlog event解析后，这个event_length字段的值也被解析出来了，转换成Event对象后，在存储到store模块时，就可以根据其值判断占用内存大小。
    // 不熟悉mysql binlog event结构的读者可参考：https://dev.mysql.com/doc/internals/en/event-structure.html
    //
    // 需要注意的是，这个计算并不精确。原始的event_length表示的是event是二进制字节流时的字节数，在转换成java对象后，基本上都会变大。
    // 如何获取java对象的真实大小，可参考这个博客：https://www.cnblogs.com/Kidezyq/p/8030098.html。
    private long calculateSize(Event event) {
        // 直接返回binlog中的事件大小
        return event.getEntry().getHeader().getEventLength();
    }

    // 进行位置转换，其内部通过位运算来快速取余数
    private int getIndex(long sequcnce) {
        return (int) sequcnce & indexMask;
    }

    // 判断event是否是ddl类型。
    // ALTER | CREATE | ERASE | RENAME | TRUNCATE | CINDEX | DINDEX
    private boolean isDdl(EventType type) {
        return type == EventType.ALTER || type == EventType.CREATE || type == EventType.ERASE
                || type == EventType.RENAME || type == EventType.TRUNCATE || type == EventType.CINDEX
                || type == EventType.DINDEX;
    }

    // ================ setter / getter ==================

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setBufferMemUnit(int bufferMemUnit) {
        this.bufferMemUnit = bufferMemUnit;
    }

    public void setBatchMode(BatchMode batchMode) {
        this.batchMode = batchMode;
    }

    public void setDdlIsolation(boolean ddlIsolation) {
        this.ddlIsolation = ddlIsolation;
    }

}
