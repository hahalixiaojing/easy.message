package easy.message.client

import easy.message.client.model.Event

interface IEventHandler {

    fun eventHandler(event: Event)
}