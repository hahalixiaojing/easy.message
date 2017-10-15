package easy.message.client

import easy.message.client.model.Message

interface IMessageHandler {

    fun onMessage(message: Message)
}