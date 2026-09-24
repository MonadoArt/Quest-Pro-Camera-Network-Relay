package dev.monadoart.qprocamservice

class ServiceController(private val nativeLibDir: String, private val port: Int = 27280) {
    data class ActionResult(val ok: Boolean, val log: String)

    suspend fun start(): ActionResult {
        val nativeDir = shellQuote(nativeLibDir)
        val script = """
            echo 'STEP stage'
            if ! cmp -s $nativeDir/libquestpro-camera-streamer-v8.so /data/local/tmp/libquestpro-camera-streamer-v8.so; then
                cp $nativeDir/libquestpro-camera-streamer-v8.so /data/local/tmp/libquestpro-camera-streamer-v8.so || exit $?
                chmod 644 /data/local/tmp/libquestpro-camera-streamer-v8.so || exit $?
            fi
            echo 'STEP log'
            if [ ! -e /data/local/tmp/questpro-live-v8.log ]; then
                touch /data/local/tmp/questpro-live-v8.log || exit $?
            fi
            chmod 666 /data/local/tmp/questpro-live-v8.log 2>/dev/null || true
            echo 'STEP inject'
            inject_output=$( $nativeDir/libqpinjector.so /data/local/tmp/libquestpro-camera-streamer-v8.so 2>&1 )
            inject_status=$?
            printf '%s\n' "${'$'}inject_output"
            if [ "${'$'}inject_status" -ne 0 ]; then exit "${'$'}inject_status"; fi
            case "${'$'}inject_output" in
                *INJECTION_OK*|*INJECTION_ALREADY_ACTIVE*) ;;
                *) exit 1 ;;
            esac
            echo 'STEP daemon'
            $nativeDir/libqprocamd.so --daemonize --port $port
        """.trimIndent()

        val result = RootShell.run(script)
        val combined = listOf(result.stdout, result.stderr).filter { it.isNotBlank() }.joinToString("\n").trim()
        val injectorAccepted = combined.contains("INJECTION_OK") || combined.contains("INJECTION_ALREADY_ACTIVE")
        return ActionResult(result.exitCode == 0 && injectorAccepted, combined)
    }

    suspend fun stop(): ActionResult {
        val result = RootShell.run("${shellQuote(nativeLibDir)}/libqprocamd.so --stop")
        val combined = listOf(result.stdout, result.stderr).filter { it.isNotBlank() }.joinToString("\n").trim()
        return ActionResult(result.exitCode == 0, combined)
    }

    suspend fun hasRoot(): Boolean = RootShell.run("id").stdout.contains("uid=0")

    private fun shellQuote(value: String): String = "'" + value.replace("'", "'\\''") + "'"
}
