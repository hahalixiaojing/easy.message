package easy.message.server

import easy.message.GroupThreadInfo
import easy.message.client.*
import java.util.*

class MockMessageApi : IMessageApi {

    val groupserver: GroupMessageServer

    constructor() {
        val topic = Topic("topic", 2)
        this.groupserver = GroupMessageServer()
        this.groupserver.add("test", arrayListOf(topic))
    }

    override fun registerThread(app: String, threadInfo: TopicThreadInfo): TopicThreadGroupInfo {
        val topicThreadList = threadInfo.threadIds.map {
            TopicThread(threadInfo.topic, it)
        }
        this.groupserver.addThread(app, topicThreadList)
        val topicGroupThread = this.groupserver.getThreadGroup(app, arrayListOf(threadInfo.topic))[0]
        return topicGroupThread.let {

            val list = it.groupThread.map {
                GroupThreadInfo(it.threadId, it.groups)
            }

            TopicThreadGroupInfo(it.topic, list)
        }
    }

    override fun getTopicOffset(app: String, topic: String): TopicOffsetInfo {
        val topicGroupOffset = this.groupserver.getOffset(app, arrayListOf(topic))[0]
        return topicGroupOffset.let {
            TopicOffsetInfo(topic, it.groupOffset)
        }
    }

    override fun selectNextEvents(eventDataRequest: EventDataRequest): List<Event> {

        return (1..2).map {
            Event(eventDataRequest.groupOffset + it * 1L, eventDataRequest.groupId, "event ${eventDataRequest.groupOffset + it}", Date().time)
        }
    }

    override fun updateTopicOffset(app: String, topicOffsetInfo: List<TopicOffsetInfo>): List<TopicOffsetInfo> {
        topicOffsetInfo.forEach { s ->
            s.offset.forEach {
                this.groupserver.updateOffset(app, s.topic, it.key, it.value)
            }
        }
        val offset = this.groupserver.getOffset(app, topicOffsetInfo.map { it.topic })
        return offset.map {
            TopicOffsetInfo(it.topic, it.groupOffset)
        }
    }

    override fun updateTopicThread(app: String, topicThreadInfoList: List<TopicThreadInfo>): List<TopicThreadGroupInfo> {
        topicThreadInfoList.forEach {
            s ->
            val list = s.threadIds.map {
                TopicThread(s.topic, it)

            }
            this.groupserver.updateThreadTimeout(app, list)
        }
        val threadGroup = this.groupserver.getThreadGroup(app, topicThreadInfoList.map { it.topic })
        return threadGroup.map {
            val list = it.groupThread.map {
                GroupThreadInfo(it.threadId, it.groups)
            }
            TopicThreadGroupInfo(it.topic, list)
        }
    }
}