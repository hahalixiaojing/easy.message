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
                println("client = 2,threadId= ${Thread.currentThread().name},groupId = ${event.groupId},id=${event.id},data =${event.data}")
            }
        })
        topicClientManager.start()

        Thread.sleep(20000)

        val topicClientManager1 = TopicClientManager("test", mockApi)
        topicClientManager1.regiserTopic("topic", object : IEventHandler {
            override fun eventHandler(event: Event) {
                println("client = 1,threadId= ${Thread.currentThread().name},groupId = ${event.groupId},id=${event.id},data =${event.data}")
            }
        })
        topicClientManager1.start()

        Thread.sleep(10000000)
    }
}