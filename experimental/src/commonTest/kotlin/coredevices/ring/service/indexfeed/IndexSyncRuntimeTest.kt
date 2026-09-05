package coredevices.ring.service.indexfeed

import kotlin.test.Test
import kotlin.test.assertFalse
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
            selfHosted = ::FakeIndexSyncRuntime,
        )

        runtime.start()

        assertFalse(firestoreResolved)
    }

    private class FakeIndexSyncRuntime : IndexSyncRuntime {
        var started = false

        override fun start() {
            started = true
        }
    }
}
