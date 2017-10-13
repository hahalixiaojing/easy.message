package easy.message.server

import com.alibaba.fastjson.JSON
import easy.message.GroupThreadInfo
import easy.message.server.model.GroupThread
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class TopicGroupThreadManager {
    //key = threadId,value= groupIds
    private val groupThread = ConcurrentHashMap<String, GroupThread>()
    private val groupCount: Int
    private val topic: String
    private val timeout = 60

    private val lock = ReentrantLock()

    constructor(topic: String, groupCount: Int) {
        this.topic = topic
        this.groupCount = groupCount
        val scheduleAtFixedRate = Executors.newSingleThreadScheduledExecutor({ r -> Thread(r, "clear-died-thread") }).scheduleAtFixedRate({

            for ((key, value) in this.groupThread) {
                val toEpochSecond = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)
                if (toEpochSecond > value.timeout.get()) {
                    this.remove(key)
                    println("timeout thread $key")
                }
            }


        }, 5, (timeout / 2).toLong(), TimeUnit.SECONDS)
    }

    fun add(threadId: String) {
        this.rebalanceGroup(threadId, 1)
    }

    fun remove(threadId: String) {
        this.rebalanceGroup(threadId, -1)
    }

    fun updateTimeout(threadId: String) {
        if (this.groupThread.containsKey(threadId)) {
            this.groupThread[threadId]?.timeout?.set(LocalDateTime.now().plusSeconds(timeout.toLong()).toEpochSecond(ZoneOffset.UTC))
        }
    }

    fun getThreadGroup(): ArrayList<GroupThreadInfo> {

        val groupThreadInfoList = ArrayList<GroupThreadInfo>()
        for ((key, value) in this.groupThread) {
            groupThreadInfoList.add(GroupThreadInfo(key, value.groups))
        }
        return groupThreadInfoList
    }

    private fun rebalanceGroup(threadId: String, addOrRemove: Int) {

        val addThreadFun = fun() {
            if (this.groupThread.isEmpty()) {
                val groups = ArrayList<Int>()
                groups += 0 until groupCount
                this.groupThread.put(threadId, GroupThread(LocalDateTime.now().plusSeconds(timeout.toLong()).toEpochSecond(ZoneOffset.UTC), groups))
                println(JSON.toJSONString(this.groupThread))
            } else {

                val groupSize: Int = Math.ceil(groupCount * 1.0 / (this.groupThread.size + 1)).toInt()

                val newGroupArray = ArrayList<Int>()
                while (true) {
                    val max = this.groupThread.maxBy { it.value.groups.size }?.value ?: break
                    if (max.groups.size <= groupSize) {
                        break
                    }
                    newGroupArray.add(max.groups.removeAt(max.groups.size - 1))
                    if (newGroupArray.size >= groupSize) {
                        break
                    }
                }
                this.groupThread.put(threadId, GroupThread(LocalDateTime.now().plusSeconds(timeout.toLong()).toEpochSecond(ZoneOffset.UTC), newGroupArray))
                println(JSON.toJSONString(this.groupThread))
            }
        }
        val removeThreadFun = fun() {
            if (this.groupThread.isEmpty()) {
                println(JSON.toJSONString(this.groupThread))
                return
            } else {
                val removeGroupArrays = this.groupThread[threadId] ?: return
                val groupSize: Int = Math.ceil(groupCount * 1.0 / (this.groupThread.size - 1)).toInt()

                while (true) {

                    val min = this.groupThread.filter { it.key != threadId }.minBy { it.value.groups.size } ?: break
                    if (min.value.groups.size >= groupSize) {
                        break
                    }

                    if (removeGroupArrays.groups.isNotEmpty()) {
                        val removeAt = removeGroupArrays.groups.removeAt(removeGroupArrays.groups.size - 1)
                        min.value.groups.add(removeAt)
                    }
                }
                this.groupThread.remove(threadId)
                println(JSON.toJSONString(this.groupThread))
            }
        }


        lock.lock()
        try {
            if (addOrRemove == 1) {
                addThreadFun()
            } else if (addOrRemove == -1) {
                removeThreadFun()
            }

        } finally {
            lock.unlock()
        }
    }
}