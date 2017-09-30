package easy.message.client

class TopicOffsetInfo {

    constructor(topic: String, offset: Map<Int, Long>) {
        this.topic = topic
        this.offset = offset
    }

    var topic: String
        get() = field
        private set(value) {
            field = value
        }
    var offset: Map<Int, Long>
        get() = field
        private set(value) {
            field = value
        }
}