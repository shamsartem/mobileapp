package coredevices.ring.service.indexfeed

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class IndexSyncRuntimeTest {
    @Test
    fun emptyUrlSelectsFirestore() {
        val firestore = FakeIndexSyncRuntime()
        val selfHosted = FakeIndexSyncRuntime()

        val runtime = selectIndexSyncRuntime("", { firestore }, { selfHosted })
        runtime.start()

        assertSame(firestore, runtime)
        assertTrue(firestore.started)
        assertFalse(selfHosted.started)
    }

    @Test
    fun urlSelectsSelfHostedWithoutResolvingFirestore() {
        var firestoreResolved = false
        val runtime = selectIndexSyncRuntime(
            backendUrl = "https://index.example.com",
            firestore = {
                firestoreResolved = true
                FakeIndexSyncRuntime()
            },
            selfHosted = ::SelfHostedIndexSyncRuntime,
        )

        runtime.start()

        assertFalse(firestoreResolved)
        assertNull(runtime.account.value)
    }

    private class FakeIndexSyncRuntime : IndexSyncRuntime {
        override val account: StateFlow<IndexSyncAccount?> = MutableStateFlow(null)
        var started = false

        override fun start() {
            started = true
        }

        override suspend fun syncNow() = Unit
    }
}
