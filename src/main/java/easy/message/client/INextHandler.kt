package easy.message.client

interface INextEventRequestHandler {
    fun next(request: EventDataRequest)
}