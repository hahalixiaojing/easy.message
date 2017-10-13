package easy.message.server

import easy.message.client.Event
import easy.message.client.IEventHandler
import easy.message.client.TopicClientManager
import org.junit.Test

class GroupTest {

    @Test
    fun group() {

        val mockApi = MockMessageApi()


        val topicClientManager = TopicClientManager("test", mockApi)

        topicClientManager.regiserTopic("topic", object : IEventHandler {
            override fun eventHandler(event: Event) {
                print("groupId = ${event.groupId},id=${event.id},data =${event.data}")
            }
        })

        topicClientManager.start()
    }
}