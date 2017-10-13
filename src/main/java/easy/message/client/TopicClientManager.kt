package easy.message.client

import com.alibaba.fastjson.JSON
import java.util.concurrent.*

class TopicClientManager {

    private val iMessageApi: IMessageApi
    private val applicationName: String
    private val eventRequestQueue = ArrayBlockingQueue<EventDataRequest>(500)
    //key=topic
    private val groupThreadMap = ConcurrentHashMap<String, TopicGroupThreadManager>()
    private var updateGroupOffsetThreadExecutor = Executors.newSingleThreadScheduledExecutor()
    private var updateThreadExecutor = Executors.newSingleThreadScheduledExecutor()
    private val eventDataExecutor: List<ScheduledExecutorService>

    constructor(applicationName: String, iMessageApi: IMessageApi) {
        this.applicationName = applicationName
        this.iMessageApi = iMessageApi
        //注册10个调http获取Event数据线程池
        this.eventDataExecutor = ArrayList<ScheduledExecutorService>()
        for (i in 1..10) {
            this.eventDataExecutor.add(Executors.newSingleThreadScheduledExecutor())
        }
    }

    /**
     * 注册topic
     */
    fun regiserTopic(topic: String, eventHandler: IEventHandler) {
        val groupThreadManager = TopicGroupThreadManager(topic, 1, eventHandler, this)
        this.groupThreadMap.put(topic, groupThreadManager)
    }

    fun start() {
        //启动,向服务端注册线程信息、获取线程和Group分配信息、获取数据GROUP 最新offset值
        for ((key, value) in groupThreadMap) {
            val threadIds = value.getThreadIds()
            val registerThread = this.iMessageApi.registerThread(this.applicationName, threadIds)
            val topicOffset = this.iMessageApi.getTopicOffset(this.applicationName, key)

            value.start(registerThread, topicOffset)
        }
        //启动获得event数据定时任务数组
        val time = arrayOf(10, 10, 8, 8, 8, 4, 4, 3, 3, 2)
        for (index in 0 until this.eventDataExecutor.size) {
            this.eventDataExecutor[index].scheduleWithFixedDelay({
                val dataRequest = this.eventRequestQueue.take()
                if (dataRequest != null) {
                    if (dataRequest.groupId == -1 || dataRequest.groupOffset == -1L) {
                        this.groupThreadMap[dataRequest.topic]?.addEvent(dataRequest.groupId, emptyList())
                    } else {
                        val selectNextEvents = this.iMessageApi.selectNextEvents(dataRequest)
                        this.groupThreadMap[dataRequest.topic]?.addEvent(dataRequest.groupId, selectNextEvents)
                    }
                }

            }, 5, time[index].toLong(), TimeUnit.SECONDS)
        }
        //向服务器更新分组offset信息,同时返回最新offset信息,并刷新本offset信息
        this.updateGroupOffsetThreadExecutor.scheduleWithFixedDelay({
            val groupOffsetList = ArrayList<TopicOffsetInfo>()
            for (item in this.groupThreadMap) {
                val groupOffset = item.value.getGroupOffset()
                groupOffsetList.add(groupOffset)
            }
            val updateTopicOffset = this.iMessageApi.updateTopicOffset(this.applicationName, groupOffsetList)
            updateTopicOffset.forEach {
                this.groupThreadMap[it.topic]?.updateGroupOffset(it)
            }

        }, 5, 3, TimeUnit.SECONDS)

        //向服务器更新线程状态,并刷新本地线程信息,主要刷新线程和GROUP关系
        this.updateThreadExecutor.scheduleWithFixedDelay({
            val threadInfoList = ArrayList<TopicThreadInfo>()
            for (item in this.groupThreadMap) {
                val threadIds = item.value.getThreadIds()
                threadInfoList.add(threadIds)
            }
            val updateTopicThread = this.iMessageApi.updateTopicThread(this.applicationName, threadInfoList)
            updateTopicThread.forEach {
                this.groupThreadMap[it.topic]?.updateGroupThread(it.groupTheadInfoList)
            }

        }, 5, 10, TimeUnit.SECONDS)
    }

    fun addNextEventQuest(eventDataRequest: EventDataRequest) {
//        java.util.concurrent.ConcurrentSkipListSet
        println(JSON.toJSONString(eventDataRequest))

        val count = this.eventRequestQueue.count { it.topic == eventDataRequest.topic && it.groupId == eventDataRequest.groupId }

        if (count == 0) {
            this.eventRequestQueue.add(eventDataRequest)
        } else {
//            println("addNextEventQuest $count > 0")
        }
    }
}