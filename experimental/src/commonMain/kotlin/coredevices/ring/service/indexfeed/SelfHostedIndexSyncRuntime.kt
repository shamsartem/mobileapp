package coredevices.ring.service.indexfeed

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow

class SelfHostedIndexSyncRuntime : IndexSyncRuntime {
    override val account: StateFlow<IndexSyncAccount?> = MutableStateFlow(null)

    override fun start() = Unit

    override suspend fun syncNow() = Unit
}
