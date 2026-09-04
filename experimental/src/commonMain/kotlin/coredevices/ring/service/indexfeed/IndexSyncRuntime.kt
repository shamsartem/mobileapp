package coredevices.ring.service.indexfeed

interface IndexSyncRuntime {
    fun start()
}

internal fun selectIndexSyncRuntime(
    backendUrl: String,
    firestore: () -> IndexSyncRuntime,
    selfHosted: () -> IndexSyncRuntime,
): IndexSyncRuntime = if (backendUrl.isBlank()) firestore() else selfHosted()
