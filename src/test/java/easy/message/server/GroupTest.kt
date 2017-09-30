package easy.message.server

import org.junit.Test
import java.time.LocalDateTime
import java.time.ZoneOffset

class GroupTest {

    @Test
    fun group() {

        val group = GroupThreadManager(10)

        group.add("1")
        Thread.sleep(4000)
        group.add("2")
        Thread.sleep(8000)
        group.add("3")
        Thread.sleep(12000)
        group.add("4")

        Thread.sleep(90000)

//        group.remove(1)
//        Thread.sleep(2000)
//        group.remove(2)
//        Thread.sleep(2000)
//        group.remove(3)
//        Thread.sleep(2000)


        val toEpochSecond = LocalDateTime.now().plusSeconds(30).toEpochSecond(ZoneOffset.UTC)
        val toEpochSecond1 = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)

        val l = toEpochSecond - toEpochSecond1
        println(l)
    }
}