package easy.message.server.model

class TopicGroupOffset {
    constructor(topic: String, groupOffset: Map<Int, Long>) {
        this.topic = topic
        this.groupOffset = groupOffset
    }

    var topic: String
        get() = field
        private set(value) {
            field = value
        }

    var groupOffset: Map<Int, Long>
        get() = field
        private set(value) {
            field = value
        }
}