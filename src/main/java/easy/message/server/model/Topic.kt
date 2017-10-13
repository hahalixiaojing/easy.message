package easy.message.server.model

class Topic {

    constructor(topicName: String, groupCount: Int) {
        this.topicName = topicName
        this.groupCount = groupCount
    }

    var topicName: String
        get() = field
        private set(value) {
            field = value
        }
    var groupCount:Int
        get() = field
        private set(value) {
            field = value
        }
}