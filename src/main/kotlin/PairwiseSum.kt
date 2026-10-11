import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

object PairwiseSum {

    fun compute(data: IntArray): Int {
        if (data.size <= 1) return if (data.isEmpty()) 0 else data[0]
        return reduce(data)
    }

    private fun reduce(data: IntArray): Int {
        val workerCount = Runtime.getRuntime().availableProcessors().coerceAtLeast(1)
        val ready = CountDownLatch(workerCount)
        val stopped = CountDownLatch(workerCount)
        val finished = CountDownLatch(1)
        val error = AtomicReference<Throwable>()
        val inboxes = Array(workerCount) { ArrayBlockingQueue<Command>(1) }

        val threads = Array(workerCount) { id ->
            Thread(Worker(data, inboxes[id], ready, stopped, error), "worker-$id")
        }
        threads.forEach { it.start() }
        ready.await()

        Thread(Controller(data, inboxes, stopped, finished, error), "controller").start()
        finished.await()
        error.get()?.let { throw it }
        return data[0]
    }

    private class Worker(
        private val data: IntArray,
        private val inbox: ArrayBlockingQueue<Command>,
        private val ready: CountDownLatch,
        private val stopped: CountDownLatch,
        private val error: AtomicReference<Throwable>,
    ) : Runnable {

        override fun run() {
            ready.countDown()
            try {
                while (true) {
                    when (val command = inbox.take()) {
                        StopCommand -> return
                        is RangeCommand -> addRange(command)
                    }
                }
            } finally {
                stopped.countDown()
            }
        }

        private fun addRange(command: RangeCommand) {
            try {
                val length = command.activeLength
                for (index in command.from until command.end) {
                    val partner = length - 1 - index
                    data[index] += data[partner]
                }
            } catch (thrown: Throwable) {
                error.compareAndSet(null, thrown)
            } finally {
                command.done.countDown()
            }
        }
    }

    private class Controller(
        private val data: IntArray,
        private val inboxes: Array<ArrayBlockingQueue<Command>>,
        private val stopped: CountDownLatch,
        private val finished: CountDownLatch,
        private val error: AtomicReference<Throwable>,
    ) : Runnable {

        override fun run() {
            try {
                var length = data.size
                while (length > 1) {
                    runWave(length)
                    error.get()?.let { throw it }
                    length -= length / 2
                }
            } catch (thrown: Throwable) {
                error.compareAndSet(null, thrown)
            } finally {
                try {
                    stopWorkers()
                } finally {
                    finished.countDown()
                }
            }
        }

        private fun runWave(length: Int) {
            val pairs = length / 2
            val workerCount = inboxes.size
            val slices = ArrayList<Slice>(workerCount)
            for (id in 0 until workerCount) {
                val from = (id.toLong() * pairs / workerCount).toInt()
                val end = ((id + 1L) * pairs / workerCount).toInt()
                if (from < end) slices.add(Slice(id, from, end))
            }
            val done = CountDownLatch(slices.size)
            for (slice in slices) {
                inboxes[slice.workerId].put(RangeCommand(slice.from, slice.end, length, done))
            }
            done.await()
        }

        private fun stopWorkers() {
            for (inbox in inboxes) {
                inbox.put(StopCommand)
            }
            stopped.await()
        }

        private class Slice(val workerId: Int, val from: Int, val end: Int)
    }

    private sealed interface Command

    private class RangeCommand(
        val from: Int,
        val end: Int,
        val activeLength: Int,
        val done: CountDownLatch,
    ) : Command

    private data object StopCommand : Command
}
