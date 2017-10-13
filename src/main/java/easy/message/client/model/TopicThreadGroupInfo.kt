package easy.message.client.model

import easy.message.GroupThreadInfo

class TopicThreadGroupInfo {
    constructor(topic: String, groupThreadInfoList: List<GroupThreadInfo>) {
        this.topic = topic
        this.groupTheadInfoList = groupThreadInfoList
    }

    var groupTheadInfoList: List<GroupThreadInfo>
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