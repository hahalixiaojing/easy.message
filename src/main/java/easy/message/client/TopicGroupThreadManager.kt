package easy.message.client

import easy.message.GroupThreadInfo
import easy.message.client.model.*
import org.apache.commons.lang3.StringUtils
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

class TopicGroupThreadManager {
    @Volatile
    private var staring = true
    private val topic: String
    private val initThreadCount: Int
    private val threads = ArrayList<Thread>() //线程集合
    private val messageHandler: IMessageHandler
    //key=threaId value= groupIds
    private val threadGroup = ConcurrentHashMap<String, ArrayList<Int>>() //记录当前线程可以消费的分组
    //key= threaId value=groupId的index
    private val threadCurrentGroupIndex = ConcurrentHashMap<String, AtomicInteger>() //记录当前线程消费的当前分组索引
    //key = threadId value=待消费的数据
    private val queue = ConcurrentHashMap<String, ArrayBlockingQueue<Message>>()
    //groupoffset 存储当前topic下所有分组offset信息
    //当前消息的分组offset由本地更新,其他消息分组通过获取服务端更新
    private val groupOffset = ConcurrentHashMap<Int, AtomicLong>()
    private val topicClientManager: TopicClientManager
    private var sendGetEventThread = Executors.newSingleThreadScheduledExecutor({ r -> Thread(r, "add-data-request-thread") })

    constructor(topic: String, initThreadCount: Int = 4, messageHandler: IMessageHandler, topicClientManager: TopicClientManager) {
        this.topic = topic
        this.initThreadCount = initThreadCount
        this.messageHandler = messageHandler
        this.topicClientManager = topicClientManager

        for (i in 0 until this.initThreadCount) {
            this.threads.add(this.createConsumerThread())
        }
        this.threads.forEach {
            this.queue[it.name] = ArrayBlockingQueue<Message>(1000)
        }
        sendGetEventThread.scheduleWithFixedDelay({
            this.threadGroup.forEach({
                if (it.value.size > 0 && this.queue[it.key]!!.size < 700) {
                    this.addRequestEventDataMessage(it.key)
                }
            })

        }, 5, 3, TimeUnit.SECONDS)
    }

    /**
     * 获得取本地线程消费的消息分组offset
     */
    fun getGroupOffset(): TopicOffsetInfo {
        //此处应该只获得本地消费的消息分组offset，去更新服务器上offset
        val localGroups = this.threadGroup.values.flatMap { it }

        val map = HashMap<Int, Long>()
        localGroups.forEach {
            val offset = this.groupOffset[it]?.get() ?: 0
            map.put(it, offset)
        }
        return TopicOffsetInfo(this.topic, map)
    }

    /**
     * 获得本地存活线程名称列表
     */
    fun getThreadIds(): TopicThreadInfo {
        val toList = this.threads.map { it.name }.toList()
        return TopicThreadInfo(this.topic, toList)
    }

    /**
     * 更新线程对应的消费分组
     */
    fun updateGroupThread(groupThreadList: List<GroupThreadInfo>) {
        this.threads.map { it.name }.forEach { t ->
            {
                val groups = groupThreadList.singleOrNull { s -> s.threadId == t }?.groups ?: ArrayList<Int>()
                if (this.threadGroup.containsKey(t)) {
                    this.threadGroup.replace(t, groups)
                } else {
                    this.threadGroup.put(t, groups)
                }


            }.invoke()
        }
    }

    /**
     * 更新消息分组的offset信息
     */
    fun updateGroupOffset(topicOffsetInfo: TopicOffsetInfo) {
        //更新分组偏移，只更新远程的，不更新本地的
        val localGroups = this.threadGroup.values.flatMap { it }

        topicOffsetInfo.offset.forEach {
            if (!localGroups.contains(it.key)) {
                this.groupOffset[it.key]?.set(it.value)
            }
        }
    }

    /**
     * 初始化消息分组消费的位置offset
     */
    fun initGroupOffset(topicOffsetInfo: TopicOffsetInfo) {
        topicOffsetInfo.offset.forEach {
            this.groupOffset[it.key] = AtomicLong(it.value)
        }

    }

    fun start(topicThreadGroupInfo: TopicThreadGroupInfo, topicOffsetInfo: TopicOffsetInfo) {

        this.updateGroupThread(topicThreadGroupInfo.groupTheadInfoList)
        this.initGroupOffset(topicOffsetInfo)
        this.initGroupIndex(topicThreadGroupInfo)
        this.threads.forEach {
            it.start()
            println("消费线程启动 threadId=${it.name}")
        }
    }

    fun close() {
        this.sendGetEventThread.shutdown()
        this.staring = false
    }

    /**
     * 将待消费的数据加入到队列
     */
    fun addEvent(groupId: Int, messageList: List<Message>) {

        val threadId = this.getThreadIdByGroupId(groupId) ?: ""
        if (StringUtils.isNotBlank(threadId)) {
            if (messageList.isNotEmpty()) {
                this.queue[threadId]?.addAll(messageList)
            }
        }
    }

    /**
     * 获得指定线程的下一个消费分组，一个线程可能消费多个分组的数据
     */
    fun getNextGroup(threadId: String): Int {
        val index = this.threadCurrentGroupIndex.getOrPut(threadId, { AtomicInteger(0) }).get()
        val groups = this.threadGroup[threadId]
        //如果下一个要消费分组索引位置大于或等于分组集合的size 则从第一个分组开始
        if (index + 1 >= groups!!.size) {
            this.threadCurrentGroupIndex[threadId]!!.set(0)
            return groups[0]
        }
        //消费下一个分组
        this.threadCurrentGroupIndex[threadId]!!.set(index + 1)
        return groups[index + 1]
    }

    /**
     * 创建消费线程
     */
    private fun createConsumerThread(): Thread {
        val thread = Thread({
            while (this.staring && !Thread.currentThread().isInterrupted) {
                val event = this.queue[Thread.currentThread().name]?.take()
                try {
                    event?.let {
                        this.messageHandler.onMessage(it)
                        this.groupOffset[it.groupId]!!.set(it.id) //记录消费offset
                    }
                } catch (e: Exception) {

                }
            }
        })
        thread.name = UUID.randomUUID().toString()
        thread.isDaemon = true
        return thread

    }

    /**
     * 初始化分组消费索引位置
     */
    private fun initGroupIndex(topicThreadGroupInfo: TopicThreadGroupInfo) {

        topicThreadGroupInfo.groupTheadInfoList.forEach {
            this.threadCurrentGroupIndex.put(it.threadId, AtomicInteger(if (it.groups.size > 0) 0 else -1))
        }
    }


    private fun getThreadIdByGroupId(groupId: Int): String? {
        return this.threadGroup.filter { it.value.contains(groupId) }.keys.firstOrNull()
    }

    /**
     * 添加请求
     */
    private fun addRequestEventDataMessage(threadId: String) {
        val nextGroup = this.getNextGroup(threadId)
        val nextGroupOffset = this.groupOffset[nextGroup]!!.get()
        this.topicClientManager.addNextEventQuest(MessageDataRequest(nextGroup, nextGroupOffset, topic))
    }
}
