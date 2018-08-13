package com.fly.sync.canal;

import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.alibaba.otter.canal.server.exception.CanalServerException;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Client {

    public final static Logger logger = LoggerFactory.getLogger(Client.class);
    private Server server;
    private ClientIdentity clientIdentity;
    private CanalInstance canalInstance;
    private final static AtomicLong CLIENT_ID = new AtomicLong(1000);

    public Client(Server server) {
        this.server = server;
        clientIdentity = new ClientIdentity(server.getCanal().getName(), (short) CLIENT_ID.incrementAndGet(), server.getFilter());
        canalInstance = server.getCanalInstance();
    }

    public ClientIdentity getClientIdentity()
    {
        return clientIdentity;
    }

    public Server getServer()
    {
        return server;
    }

    public CanalInstance getCanalInstance()
    {
        return canalInstance;
    }

    private void checkStart() throws CanalServerException
    {
        if (!canalInstance.getMetaManager().isStart())
            throw new CanalServerException("Cannal server ["+ server.getCanal().getName() +"] is stop.");
    }

    private void checkSubscribe() {

        boolean hasSubscribe = canalInstance.getMetaManager().hasSubscribe(clientIdentity);
        if (!hasSubscribe) {
            throw new CanalServerException(String.format("ClientIdentity:%s should subscribe first",
                    clientIdentity.toString()));
        }
    }

    /**
     * 查询所有的订阅信息
     */
    public List<ClientIdentity> listAllSubscribe() throws CanalServerException
    {
        return canalInstance.getMetaManager().listAllSubscribeInfo(server.getCanal().getName());
    }

    /**
     * 查询当前未被ack的batch列表，batchId会按照从小到大进行返回
     */
    public List<Long> listBatchIds() throws CanalServerException {
        checkStart();
        checkSubscribe();

        Map<Long, PositionRange> batchs = canalInstance.getMetaManager().listAllBatchs(clientIdentity);
        List<Long> result = new ArrayList<>(batchs.keySet());
        Collections.sort(result);
        return result;
    }

    public void subscribe() throws CanalServerException
    {
        checkStart();

        canalInstance.getMetaManager().subscribe(clientIdentity); // 执行一下meta订阅
        Position position = canalInstance.getMetaManager().getCursor(clientIdentity);

        if (position == null) {
            position = canalInstance.getEventStore().getFirstPosition();// 获取一下store中的第一条
            if (position != null) {
                canalInstance.getMetaManager().updateCursor(clientIdentity, position); // 更新一下cursor
            }
            logger.info("subscribe successfully, {} with first position:{} ", clientIdentity, position);
        } else {
            logger.info("subscribe successfully, use last cursor position:{} ", clientIdentity, position);
        }

        canalInstance.subscribeChange(clientIdentity);
    }

    public void unsubscribe() throws CanalServerException
    {
        canalInstance.getMetaManager().unsubscribe(clientIdentity); // 执行一下meta订阅

        logger.info("unsubscribe successfully, {}", clientIdentity);
    }

    /**
     * 获取数据
     *
     * <pre>
     * 注意： meta获取和数据的获取需要保证顺序性，优先拿到meta的，一定也会是优先拿到数据，所以需要加同步. (不能出现先拿到meta，拿到第二批数据，这样就会导致数据顺序性出现问题)
     * </pre>
     */
    public Message get(int batchSize) throws CanalServerException {
        return get(batchSize, null, null);
    }

    /**
     * 获取数据，可以指定超时时间.
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
    public Message get(int batchSize, Long timeout, TimeUnit unit)
            throws CanalServerException {

        checkStart();
        checkSubscribe();

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
                logger.trace("get successfully, clientId:{} batchSize:{} but result is null", new Object[] {
                        clientIdentity.getClientId(), batchSize });
                return new Message(-1, new ArrayList<CanalEntry.Entry>()); // 返回空包，避免生成batchId，浪费性能
            } else {
                // 记录到流式信息
                Long batchId = canalInstance.getMetaManager().addBatch(clientIdentity, events.getPositionRange());
                List<CanalEntry.Entry> entrys = Lists.transform(events.getEvents(), input -> input.getEntry());

                logger.info("get successfully, clientId:{} batchSize:{} real size is {} and result is [batchId:{} , position:{}]",
                        clientIdentity.getClientId(),
                        batchSize,
                        entrys.size(),
                        batchId,
                        events.getPositionRange());
                // 直接提交ack
                ack(batchId);
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
    public Message getWithoutAck(int batchSize) throws CanalServerException {
        return getWithoutAck(batchSize, null, null);
    }

    /**
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
     * 注意： meta获取和数据的获取需要保证顺序性，优先拿到meta的，一定也会是优先拿到数据，所以需要加同步. (不能出现先拿到meta，拿到第二批数据，这样就会导致数据顺序性出现问题)
     * </pre>
     */
    public Message getWithoutAck(int batchSize, Long timeout, TimeUnit unit)
            throws CanalServerException {
        checkStart();
        checkSubscribe();

        synchronized (canalInstance) {
            // 获取到流式数据中的最后一批获取的位置
            PositionRange<LogPosition> positionRanges = canalInstance.getMetaManager().getLastestBatch(clientIdentity);

            Events<Event> events = null;
            if (positionRanges != null) { // 存在流数据
                events = getEvents(canalInstance.getEventStore(), positionRanges.getStart(), batchSize, timeout, unit);
            } else {// ack后第一次获取
                Position start = canalInstance.getMetaManager().getCursor(clientIdentity);
                if (start == null) { // 第一次，还没有过ack记录，则获取当前store中的第一条
                    start = canalInstance.getEventStore().getFirstPosition();
                }

                events = getEvents(canalInstance.getEventStore(), start, batchSize, timeout, unit);
            }

            if (CollectionUtils.isEmpty(events.getEvents())) {
                logger.trace("getWithoutAck successfully, clientId:{} batchSize:{} but result is null", new Object[] {
                        clientIdentity.getClientId(), batchSize });
                return new Message(-1, new ArrayList<CanalEntry.Entry>()); // 返回空包，避免生成batchId，浪费性能
            } else {
                // 记录到流式信息
                Long batchId = canalInstance.getMetaManager().addBatch(clientIdentity, events.getPositionRange());
                List<CanalEntry.Entry> entrys = Lists.transform(events.getEvents(),  input -> input.getEntry());

                logger.trace("getWithoutAck successfully, clientId:{} batchSize:{}  real size is {} and result is [batchId:{} , position:{}]",
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
     * 进行 batch id 的确认。确认之后，小于等于此 batchId 的 Message 都会被确认。
     *
     * <pre>
     * 注意：进行反馈时必须按照batchId的顺序进行ack(需有客户端保证)
     * </pre>
     */
    public void ack(long batchId) throws CanalServerException {
        checkStart();
        checkSubscribe();

        PositionRange<LogPosition> positionRanges = null;
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

        // 更新cursor
        if (positionRanges.getAck() != null) {
            canalInstance.getMetaManager().updateCursor(clientIdentity, positionRanges.getAck());
            logger.trace("ack successfully, clientId:{} batchId:{} position:{}",
                    clientIdentity.getClientId(),
                    batchId,
                    positionRanges);
        }

        // 可定时清理数据
        canalInstance.getEventStore().ack(positionRanges.getEnd());

    }

    /**
     * 回滚到未进行 {@link #ack} 的地方，下次fetch的时候，可以从最后一个没有 {@link #ack} 的地方开始拿
     */
    public void rollback() throws CanalServerException {
        checkStart();
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
            logger.info("rollback successfully, clientId:{}", new Object[] { clientIdentity.getClientId() });
        }
    }

    /**
     * 回滚到未进行 {@link #ack} 的地方，下次fetch的时候，可以从最后一个没有 {@link #ack} 的地方开始拿
     */
    public void rollback(Long batchId) throws CanalServerException {
        checkStart();

        // 因为存在第一次链接时自动rollback的情况，所以需要忽略未订阅
        boolean hasSubscribe = canalInstance.getMetaManager().hasSubscribe(clientIdentity);
        if (!hasSubscribe) {
            return;
        }
        synchronized (canalInstance) {
            // 清除batch信息
            PositionRange<LogPosition> positionRanges = canalInstance.getMetaManager().removeBatch(clientIdentity,
                    batchId);
            if (positionRanges == null) { // 说明是重复的ack/rollback
                throw new CanalServerException(String.format("rollback error, clientId:%s batchId:%d is not exist , please check",
                        clientIdentity.getClientId(),
                        batchId));
            }

            // lastRollbackPostions.put(clientIdentity,
            // positionRanges.getEnd());// 记录一下最后rollback的位置
            // TODO 后续rollback到指定的batchId位置
            canalInstance.getEventStore().rollback();// rollback
            // eventStore中的状态信息
            logger.info("rollback successfully, clientId:{} batchId:{} position:{}",
                    clientIdentity.getClientId(),
                    batchId,
                    positionRanges);
        }
    }

    /**
     * 根据不同的参数，选择不同的方式获取数据
     */
    private Events<Event> getEvents(CanalEventStore eventStore, Position start, int batchSize, Long timeout,
                                    TimeUnit unit) {
        if (timeout == null) {
            return eventStore.tryGet(start, batchSize);
        } else {
            try {
                if (timeout <= 0) {
                    return eventStore.get(start, batchSize);
                } else {
                    return eventStore.get(start, batchSize, timeout, unit);
                }
            } catch (Exception e) {
                throw new CanalServerException(e);
            }
        }
    }

}
