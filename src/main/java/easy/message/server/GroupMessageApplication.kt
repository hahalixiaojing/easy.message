package easy.message.server

import java.util.concurrent.ConcurrentHashMap

class GroupMessageApplication {
    //key = topic value=offset
    private val offsetManager = ConcurrentHashMap<String, GroupOffsetManager>()
    private val groupThreadManager = ConcurrentHashMap<String, GroupThreadManager>()
    private val applicationName: String

    constructor(applicationName: String, topicList: List<Topic>) {
        this.applicationName = applicationName

        for (topic in topicList) {

            val groupOffsetManager = GroupOffsetManager(topic.topicName)
            for (i in 0 until topic.groupCount) {
                groupOffsetManager.addOffset(i, 0)
            }
            this.offsetManager.put(topic.topicName, groupOffsetManager)
            this.groupThreadManager.put(topic.topicName, GroupThreadManager(topic.topicName, topic.groupCount))
        }
    }

    /**
     * 更新分组消费点
     */
    fun updateOffset(topic: String, group: Int, offsetValue: Long) {
        if (this.offsetManager.containsKey(topic)) {
            this.offsetManager[topic]?.addOffset(group, offsetValue)
        }
    }

    /**
     * 添加线程
     */
    fun addThread(topicThreads: List<TopicThread>) {
        topicThreads.forEach {
            this.groupThreadManager[it.topic]?.add(it.threadId)
        }
    }

    /**
     * 移除线程
     */
    fun removeThread(topicThreads: List<TopicThread>) {
        topicThreads.forEach {
            this.groupThreadManager[it.topic]?.remove(it.threadId)
        }
    }

    /**
     * 更新线程超时时间
     */
    fun updateThreadTimeout(topicThreads: List<TopicThread>) {
        topicThreads.forEach {
            this.groupThreadManager[it.topic]?.updateTimeout(it.threadId)
        }
    }

    /**
     * 获得分组消费点
     */
    fun getOffset(topic: List<String>): List<TopicGroupOffset> {
        val topicGroupOffset = ArrayList<TopicGroupOffset>()
        topic.forEach {

            val groupOffset = this.offsetManager[it]?.getGroupOffset()
            if (groupOffset != null) {
                topicGroupOffset.add(TopicGroupOffset(it, groupOffset))
            }
        }
        return topicGroupOffset
    }

    /**
     * 获得线程和分组关系
     */
    fun getThreadGroup(topic: List<String>): ArrayList<TopicGroupThread> {

        val group = ArrayList<TopicGroupThread>()
        topic.forEach {

            val threadGroup = this.groupThreadManager[it]?.getThreadGroup()

            if (threadGroup != null) {
                group.add(TopicGroupThread(it, threadGroup))
            }
        }
        return group
    }
}