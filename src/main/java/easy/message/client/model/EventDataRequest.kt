package easy.message.client.model

class EventDataRequest {

    constructor(groupId: Int, groupOffset: Long, topic: String) {
        this.groupId = groupId
        this.groupOffset = groupOffset
        this.topic = topic
    }

    var groupId: Int
        get() = field
        private set(value) {
            field = value
        }
    var groupOffset: Long
        get() = field
        private set(value) {
            field = value
        }
    var topic: String
        get() = field
        private set(value) {
            field = value
        }
}