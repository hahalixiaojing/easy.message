package easy.message.client.model

class TopicThreadInfo {
    constructor(topic: String, threadIds: List<String>) {
        this.topic = topic
        this.threadIds = threadIds
    }

    var topic: String
        get() = field
        private set(value) {
            field = value
        }
    var threadIds: List<String>
        get() = field
        private set(value) {
            field = value
        }
}