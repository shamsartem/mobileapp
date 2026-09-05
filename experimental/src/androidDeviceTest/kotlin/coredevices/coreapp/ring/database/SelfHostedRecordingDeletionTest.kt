package coredevices.coreapp.ring.database

import androidx.room.Room
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.test.platform.app.InstrumentationRegistry
import com.russhwolf.settings.MapSettings
import com.russhwolf.settings.Settings
import coredevices.indexai.data.entity.LocalRecording
import coredevices.ring.BuildKonfig
import coredevices.ring.database.firestore.dao.FirestoreRecordingsDao
import coredevices.ring.database.room.RingDatabase
import coredevices.ring.database.room.repository.RecordingRepository
import coredevices.ring.selfhosted.sync.SelfHostedSyncState
import coredevices.ring.service.RecordingBackgroundScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.Assume.assumeTrue
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.time.Clock
import kotlin.time.Duration.Companion.days

class SelfHostedRecordingDeletionTest {
    @Test
    fun deletionIsJournaledBeforeRowRemovalAndSurvivesRestart() = runBlocking<Unit> {
        assumeTrue(BuildKonfig.SELF_HOSTED_BACKEND_URL.isNotBlank())
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val db = Room.inMemoryDatabaseBuilder<RingDatabase>(context)
            .setDriver(BundledSQLiteDriver()).setQueryCoroutineContext(Dispatchers.IO).build()
        val durableSettings = MapSettings()
        var failWrites = true
        val settings = object : Settings by durableSettings {
            override fun putString(key: String, value: String) {
                if (failWrites) error("Storage unavailable")
                durableSettings.putString(key, value)
            }
        }
        val state = SelfHostedSyncState(settings)
        val repository = RecordingRepository(
            db.localRecordingDao(), db.recordingEntryDao(),
            FirestoreRecordingsDao { error("Self-hosted deletion must not access Firebase") },
            RecordingBackgroundScope(this), db, state,
        )
        try {
            val future = Clock.System.now() + 1.days
            val id = db.localRecordingDao().insertRecording(LocalRecording(firestoreId = "delete-me", updated = future))
            assertFailsWith<IllegalStateException> { repository.deleteRecording(id) }
            assertNotNull(db.localRecordingDao().getRecording(id))
            assertEquals(emptyList(), state.pendingRecordingDeletions)

            failWrites = false
            repository.deleteRecording(id)
            assertNull(db.localRecordingDao().getRecording(id))
            val deletion = SelfHostedSyncState(durableSettings).pendingRecordingDeletions.single()
            assertEquals("delete-me", deletion.id)
            assertEquals(future.toEpochMilliseconds() + 1, deletion.deletedAt)
            repository.deleteRecording(id)
            assertEquals(listOf(deletion), state.pendingRecordingDeletions)
        } finally {
            db.close()
        }
    }
}
