package coredevices.ring.service.indexfeed

import kotlinx.coroutines.flow.StateFlow

data class IndexSyncAccount(
    val id: String,
    val email: String?,
)

interface IndexSyncRuntime {
    val account: StateFlow<IndexSyncAccount?>

    fun start()

    suspend fun syncNow()
}

internal fun selectIndexSyncRuntime(
    backendUrl: String,
    firestore: () -> IndexSyncRuntime,
    selfHosted: () -> IndexSyncRuntime,
): IndexSyncRuntime = if (backendUrl.isBlank()) firestore() else selfHosted()
