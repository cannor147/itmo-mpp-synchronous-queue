import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.Continuation
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

class SynchronousQueueMS<E> : SynchronousQueue<E> {
    private val retry = Node<E>(null)

    private val head: AtomicReference<Node<E>>
    private val tail: AtomicReference<Node<E>>

    init {
        val dummy = Node<E>(null)
        this.head = AtomicReference(dummy)
        this.tail = AtomicReference(dummy)
    }

    override suspend fun send(element: E) {
        internalRun(Action.SEND, element)
    }

    override suspend fun receive(): E {
        return internalRun(Action.RECEIVE)!!
    }

    private suspend fun internalRun(action: Action, element: E? = null): E? {
        while (true) {
            val currentTail = tail.get()
            val currentHead = head.get()

            if (currentHead == currentTail || currentTail.checkAction(action)) {
                val next = currentTail.next.get()
                if (next != null) {
                    tail.compareAndSet(currentTail, next)
                } else {
                    var myNode: Node<E>? = null
                    val res = suspendCoroutine<Node<E>> sc@{ continuation ->
                        myNode = Node(element to continuation)

                        if (currentTail.next.compareAndSet(next, myNode)) {
                            tail.compareAndSet(currentTail, myNode)
                        } else {
                            continuation.resume(retry)
                            return@sc
                        }
                    }
                    if (res === retry) {
                        continue
                    } else {
                        val h = head.get()
                        if (myNode!! == h.next.get()) {
                            head.compareAndSet(h, myNode)
                        }
                        return res.data.first
                    }
                }
            } else {
                val next = currentHead.next.get()
                if (currentTail != tail.get() || currentHead != head.get() || next == null) {
                    continue
                }

                if (!next.checkAction(action) && head.compareAndSet(currentHead, next)) {
                    val (result, continuation) = next.data
                    continuation!!.resume(Node<E>(element))
                    return result
                }
            }
        }
    }

    companion object {
        private class Node<E>(val data: Pair<E?, Continuation<Node<E>>?>) {
            val next = AtomicReference<Node<E>>(null)

            constructor(data: E?) : this(data to null)

            fun checkAction(action: Action) = when (action) {
                Action.SEND -> data.first != null
                Action.RECEIVE -> data.first == null
            }
        }

        private enum class Action {
            SEND, RECEIVE
        }
    }
}
