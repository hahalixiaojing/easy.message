package easy.message.server

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class GroupOffsetManager(private val topic: String) {
    //key =  groupId,value = offset
    private val groupOffset = ConcurrentHashMap<Int, AtomicLong>()

    fun addOffset(groupId: Int, offsetValue: Long) {
        this.groupOffset.getOrPut(groupId, { AtomicLong(0) }).set(offsetValue)
    }

    fun getGroupOffset(): Map<Int, Long> {

        val map = HashMap<Int, Long>()
        for ((key, value) in this.groupOffset) {
            map.put(key, value.get())
        }
        return map
    }
}