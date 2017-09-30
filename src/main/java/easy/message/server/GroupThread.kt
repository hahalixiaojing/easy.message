package easy.message.server

import java.util.concurrent.atomic.AtomicLong

class GroupThread {
    constructor(timout: Long, groups: ArrayList<Int>) {
        this.timeout = AtomicLong(timout)
        this.groups = groups
    }

    var timeout: AtomicLong
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