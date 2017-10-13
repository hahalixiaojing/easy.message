package easy.message.server

import java.util.concurrent.ConcurrentHashMap

class GroupMessageServer {

    private val application = ConcurrentHashMap<String, GroupMessageApplication>()

    /**
     * 添加应用
     */
    fun add(applicationName: String, topicList: ArrayList<Topic>) {
        this.application[applicationName] = GroupMessageApplication(applicationName, topicList)
    }


    /**
     * 更新分组消费点
     */
    fun updateOffset(applicationName: String, topic: String, group: Int, offsetValue: Long) {
        this.application[applicationName]?.updateOffset(topic, group, offsetValue)
    }

    /**
     * 添加线程
     */
    fun addThread(applicationName: String, topicThreads: List<TopicThread>) {

        this.application[applicationName]?.addThread(topicThreads)
    }

    /**
     * 移除线程
     */
    fun removeThread(applicationName: String, topicThreads: ArrayList<TopicThread>) {

        this.application[applicationName]?.removeThread(topicThreads)
    }

    /**
     * 更新线程超时时间
     */
    fun updateThreadTimeout(applicationName: String, topicThreads: List<TopicThread>) {

        this.application[applicationName]?.updateThreadTimeout(topicThreads)
    }

    /**
     * 获得分组消费点
     */
    fun getOffset(applicationName: String, topic: List<String>): List<TopicGroupOffset> {
        return this.application[applicationName]?.getOffset(topic) ?: emptyList()
    }


    /**
     * 获得线程和分组关系
     */
    fun getThreadGroup(applicationName: String, topic: List<String>): List<TopicGroupThread> {
        return this.application[applicationName]?.getThreadGroup(topic) ?: emptyList()
    }
}