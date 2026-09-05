package coredevices.ring.selfhosted.sync

import com.russhwolf.settings.Settings
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.Json

class SelfHostedSyncState(private val settings: Settings) {
    companion object {
        internal const val CURSOR_KEY = "self_hosted_index_sync_cursor"
        internal const val RECORDING_DELETIONS_KEY = "self_hosted_index_recording_deletions"

        private val json = Json { ignoreUnknownKeys = true }
    }

    var cursor = settings.getLong(CURSOR_KEY, 0).coerceAtLeast(0)
        private set

    var pendingRecordingDeletions = loadRecordingDeletions()
        private set

    private val mutex = Mutex()

    suspend fun advanceCursor(newCursor: Long) = mutex.withLock {
        if (newCursor > cursor) {
            settings.putLong(CURSOR_KEY, newCursor)
            cursor = newCursor
        }
    }

    suspend fun addRecordingDeletion(deletion: RecordingDeletionMutation) {
        require(deletion.id.isNotBlank())
        require(deletion.deletedAt >= 0)
        mutex.withLock {
            if (pendingRecordingDeletions.none { it.id == deletion.id }) {
                storeRecordingDeletions(pendingRecordingDeletions + deletion)
            }
        }
    }

    suspend fun removeRecordingDeletion(id: String) = mutex.withLock {
        val remaining = pendingRecordingDeletions.filterNot { it.id == id }
        if (remaining.size != pendingRecordingDeletions.size) {
            storeRecordingDeletions(remaining)
        }
    }

    private fun loadRecordingDeletions(): List<RecordingDeletionMutation> =
        settings.getStringOrNull(RECORDING_DELETIONS_KEY)
            ?.let {
                try {
                    json.decodeFromString<List<RecordingDeletionMutation>>(it)
                        .filter { deletion -> deletion.id.isNotBlank() && deletion.deletedAt >= 0 }
                        .distinctBy(RecordingDeletionMutation::id)
                } catch (_: Exception) {
                    null
                }
            }
            ?: emptyList()

    private fun storeRecordingDeletions(deletions: List<RecordingDeletionMutation>) {
        settings.putString(RECORDING_DELETIONS_KEY, json.encodeToString(deletions))
        pendingRecordingDeletions = deletions
    }
}
