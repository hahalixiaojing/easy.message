package easy.message.client.model

class Message {

    constructor(id: Long, groupId: Int, text: String, timestamps: Long) {
        this.id = id
        this.groupId = groupId
        this.text = text
        this.timestamps = timestamps
    }

    var id: Long
        get() = field
        private set(value) {
            field = value
        }
    var groupId: Int
        get() = field
        private set(value) {
            field = value
        }
    var text: String
        get() = field
        private set(value) {
            field = value
        }
    var timestamps: Long
        get() = field
        private set(value) {
            field = value
        }
}