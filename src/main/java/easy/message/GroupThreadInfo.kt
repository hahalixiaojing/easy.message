package easy.message

class GroupThreadInfo {
    constructor(threadId: String, groups: ArrayList<Int>) {
        this.threadId = threadId
        this.groups = groups
    }

    var threadId:String
        get() = field
        private set(value) {
            field = value
        }
    var groups: ArrayList<Int>
        get() = field
        private set(value) {
            field = value
        }
}