package easy.message.server

import easy.message.GroupThreadInfo

class TopicGroupThread {

    constructor(topic: String, groupThread: ArrayList<GroupThreadInfo>) {
        this.topic = topic
        this.groupThread = groupThread
    }

    var topic:String
    var groupThread:ArrayList<GroupThreadInfo>
}