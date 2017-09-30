package easy.message.client

import easy.message.GroupThreadInfo
import org.apache.commons.lang3.StringUtils
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet

class GroupThreadManager {
    private val topic: String
    private val initThreadCount: Int
    private val threads = ArrayList<Thread>() //线程集合
    private val eventHandler: IEventHandler
    //key=threaId value= groupIds
    private val threadGroup = ConcurrentHashMap<String, ArrayList<Int>>() //记录当前线程可以消费的分组
    //key= threaId value=groupId的index
    private val threadCurrentGroupIndex = ConcurrentHashMap<String, AtomicInteger>() //记录当前线程消费的当前分组
    //key = threadId value=待消费的数据
    private val queue = ConcurrentHashMap<String, ArrayBlockingQueue<Event>>()
    /**
     * groupoffset 存储当前topic下所有分组offset信息
     * 当前消息的分组offset由本地更新,其他消息分组通过获取服务端更新
     */
    private val groupOffset = ConcurrentHashMap<Int, AtomicLong>()
    private val topicClientManager: TopicClientManager

    constructor(topic: String, initThreadCount: Int = 4, eventHandler: IEventHandler, topicClientManager: TopicClientManager) {
        this.topic = topic
        this.initThreadCount = initThreadCount
        this.eventHandler = eventHandler
        this.topicClientManager = topicClientManager

        for (i in 0 until this.initThreadCount) {
            this.threads.add(this.createThread())
        }
        this.threads.forEach {
            this.queue[it.name] = ArrayBlockingQueue<Event>(1000)
        }
    }

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

    fun getThreadIds(): TopicThreadInfo {
        val toList = this.threads.map { it.name }.toList()
        return TopicThreadInfo(this.topic, toList)
    }

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

    fun updateGroupOffset(topicOffsetInfo: TopicOffsetInfo) {
        //更新分组偏移，只更新远程的，不更新本地的
        val localGroups = this.threadGroup.values.flatMap { it }

        topicOffsetInfo.offset.forEach {
            if (!localGroups.contains(it.key)) {
                this.groupOffset[it.key]?.set(it.value)
            }
        }
    }
    fun initGroupOffset(){
        //todo 
    }

    fun start(topicThreadGroupInfo: TopicThreadGroupInfo, topicOffsetInfo: TopicOffsetInfo) {

        this.updateGroupThread(topicThreadGroupInfo.groupTheadInfoList)
        this.updateGroupOffset(topicOffsetInfo)
//        Thread.sleep(20000)
        this.threads.forEach {
            it.start()
        }
    }

    private fun nextEventDataRequest(threadId: String) {
        val nextGroup = this.getNextGroup(threadId)
        val nextGroupOffset = this.groupOffset[nextGroup]!!.get()
        this.topicClientManager.addNextEventQuest(EventDataRequest(nextGroup, nextGroupOffset, topic))
    }

    private fun createThread(): Thread {
        val thread = Thread({
            this.nextEventDataRequest(Thread.currentThread().name)
            while (true) {

                val eventList = ArrayList<Event>()
                this.queue[Thread.currentThread().name]?.drainTo(eventList)

                eventList.forEach {
                    try {
                        this.eventHandler.eventHandler(it)
                        //TODO:需要更新offset
                        this.groupOffset[it.groupId]!!.set(it.id) //记录消费offset

                    } catch (ex: Exception) {

                    } finally {
                        this.nextEventDataRequest(Thread.currentThread().name)
                    }
                }
            }
        })
        thread.name = UUID.randomUUID().toString()
        thread.isDaemon = true
        return thread

    }

    fun addEvent(groupId: Int, eventList: List<Event>) {

        val threadId = this.getThreadIdByGroupId(groupId) ?: ""
        if (StringUtils.isNotBlank(threadId)) {
            if (eventList.isNotEmpty()) {
                this.queue[threadId]?.addAll(eventList)
            } else {
                this.nextEventDataRequest(threadId)
            }
        }
    }

    private fun getThreadIdByGroupId(groupId: Int): String? {
        return this.threadGroup.filter { it.value.contains(groupId) }.keys.firstOrNull()
    }

    /**
     * 获得下一个消费分组
     */
    fun getNextGroup(threadId: String): Int {
        val index = this.threadCurrentGroupIndex.getOrPut(threadId, { AtomicInteger(0) }).get()
        val groups = this.threadGroup[threadId]
        if (index + 1 > groups?.size ?: 0) {
            return this.threadGroup[threadId]?.get(0) ?: -1
        }
        return groups?.get(index + 1) ?: -1
    }
}
