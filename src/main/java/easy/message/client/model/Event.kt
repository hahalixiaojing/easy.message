package easy.message.client.model

class Event {

    constructor(id: Long, groupId: Int, data: String, timestamps: Long) {
        this.id = id
        this.groupId = groupId
        this.data = data
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
    var data: String
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