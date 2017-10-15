package easy.message.server

import easy.message.client.model.Message
import easy.message.client.IMessageHandler
import easy.message.client.TopicClientManager
import org.junit.Test
import java.util.concurrent.ArrayBlockingQueue

class GroupTest {

    @Test
    fun group() {

        val mockApi = MockMessageApi()


        val topicClientManager = TopicClientManager("test", mockApi)

        topicClientManager.regiserTopic("topic", object : IMessageHandler {
            override fun onMessage(message: Message) {
                println("client = 2,threadId= ${Thread.currentThread().name},groupId = ${message.groupId},id=${message.id},data =${message.text}")
            }
        })
        topicClientManager.start()

        Thread.sleep(20000)

        val topicClientManager1 = TopicClientManager("test", mockApi)
        topicClientManager1.regiserTopic("topic", object : IMessageHandler {
            override fun onMessage(message: Message) {
                println("client = 1,threadId= ${Thread.currentThread().name},groupId = ${message.groupId},id=${message.id},data =${message.text}")
            }
        })
        topicClientManager1.start()

        Thread.sleep(10000)

        topicClientManager1.close()
        println("关闭个消费节点")



        Thread.sleep(10000000)
    }

    @Test
    fun test() {
        val ara = ArrayBlockingQueue<String>(1000)
        val take = ara.take()
        println("take size is")
    }

    @Test
    fun threadTest2() {
        val t = Thread({

            val array = ArrayBlockingQueue<String>(1000)
            while (!Thread.currentThread().isInterrupted) {
                try {
                    array.take()
                } catch (ex: Exception) {
                    println(ex)
                }

            }
            println("Interrupted")

        })

        t.start()

        Thread.sleep(2000)

        t.interrupt()

        Thread.sleep(3000)
    }
}