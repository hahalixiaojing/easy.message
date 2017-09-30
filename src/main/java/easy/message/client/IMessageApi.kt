package easy.message.client

interface IMessageApi {

    fun registerThread(app: String, threadInfo: TopicThreadInfo): TopicThreadGroupInfo
    fun getTopicOffset(app: String, topic: String): TopicOffsetInfo
    fun selectNextEvents(eventDataRequest: EventDataRequest): List<Event>
    fun updateTopicOffset(app: String, topicOffsetInfo: List<TopicOffsetInfo>):List<TopicOffsetInfo>
    fun updateTopicThread(app: String, topicThreadInfoList: List<TopicThreadInfo>): List<TopicThreadGroupInfo>
}