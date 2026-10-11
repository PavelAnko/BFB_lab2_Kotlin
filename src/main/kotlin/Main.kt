import java.util.Locale
import kotlin.system.measureNanoTime

private const val ARRAY_SIZE = 1_000_000_000
private const val ELEMENT = 1

fun main() {
    val array = try {
        IntArray(ARRAY_SIZE) { ELEMENT }
    } catch (e: OutOfMemoryError) {
        System.err.println("OOM: run with -Xmx6g")
        throw e
    }

    var sum = 0
    val timeMs = measureNanoTime { sum = PairwiseSum.compute(array) } / 1_000_000.0
    println("sum=$sum, timeMs=${"%.2f".format(Locale.US, timeMs)}")
}
