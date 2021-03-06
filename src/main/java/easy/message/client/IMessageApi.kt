package easy.message.client

import easy.message.client.model.*

interface IMessageApi {

    fun registerThread(app: String, threadInfo: TopicThreadInfo): TopicThreadGroupInfo
    fun getTopicOffset(app: String, topic: String): TopicOffsetInfo
    fun removeThread(app: String, topicThreadList: List<TopicThreadInfo>)
    fun selectNextEvents(eventDataRequest: MessageDataRequest): List<Message>
    fun updateTopicOffset(app: String, topicOffsetInfo: List<TopicOffsetInfo>): List<TopicOffsetInfo>
    fun updateTopicThread(app: String, topicThreadInfoList: List<TopicThreadInfo>): List<TopicThreadGroupInfo>
}