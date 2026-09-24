package dev.monadoart.qprocamservice

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.concurrent.TimeUnit

object RootShell {
    data class Result(val exitCode: Int, val stdout: String, val stderr: String)

    suspend fun run(script: String, timeoutMs: Long = 20_000): Result = withContext(Dispatchers.IO) {
        val process = try {
            ProcessBuilder("su", "-c", script).start()
        } catch (e: IOException) {
            return@withContext Result(-2, "", e.message ?: e.toString())
        }

        val stdoutBytes = ByteArrayOutputStream()
        val stderrBytes = ByteArrayOutputStream()
        val stdoutThread = Thread { process.inputStream.use { it.copyTo(stdoutBytes) } }
        val stderrThread = Thread { process.errorStream.use { it.copyTo(stderrBytes) } }
        stdoutThread.start()
        stderrThread.start()

        val completed = try {
            process.waitFor(timeoutMs.coerceAtLeast(0), TimeUnit.MILLISECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            false
        }

        if (!completed) {
            process.destroyForcibly()
            try {
                process.waitFor()
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }

        try {
            stdoutThread.join()
            stderrThread.join()
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }

        Result(
            if (completed) process.exitValue() else -1,
            stdoutBytes.toString(Charsets.UTF_8.name()),
            stderrBytes.toString(Charsets.UTF_8.name())
        )
    }
}
