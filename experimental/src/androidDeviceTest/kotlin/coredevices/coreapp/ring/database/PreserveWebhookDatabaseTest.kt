package coredevices.coreapp.ring.database

import androidx.room.Room
import androidx.sqlite.SQLiteConnection
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.sqlite.execSQL
import androidx.test.platform.app.InstrumentationRegistry
import coredevices.indexai.data.entity.LocalRecording
import coredevices.ring.data.entity.room.indexfeed.CachedItem
import coredevices.ring.database.room.RingDatabase
import coredevices.ring.selfhosted.PreserveWebhookDatabase
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.Test
import java.io.File
import java.util.UUID
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.time.Instant

class PreserveWebhookDatabaseTest {
    private val context = InstrumentationRegistry.getInstrumentation().targetContext
    private fun open(path: File): RingDatabase = Room.databaseBuilder<RingDatabase>(context, path.absolutePath)
        .addMigrations(PreserveWebhookDatabase)
        .setDriver(BundledSQLiteDriver()).setQueryCoroutineContext(Dispatchers.IO).build()

    private fun SQLiteConnection.scalar(sql: String): Long = prepare(sql).use {
        check(it.step())
        it.getLong(0)
    }

    private suspend fun fixture(path: File, version: Int) {
        val db = open(path)
        try {
            db.localRecordingDao().insertRecording(LocalRecording(
                id = 1, firestoreId = "retained-recording", localTimestamp = Instant.fromEpochMilliseconds(1000),
            ))
            db.cachedItemDao().upsert(CachedItem(firestoreId = "retained-item", title = "Private fixture"))
        } finally {
            db.close()
        }
        BundledSQLiteDriver().open(path.absolutePath).use {
            // The installed webhook build has an older table shape than today's webhook branch.
            it.execSQL("CREATE TABLE IndexWebhookDeliveryEntity (id INTEGER PRIMARY KEY, filename TEXT, audioData BLOB, headersJson TEXT, status TEXT)")
            it.execSQL("INSERT INTO IndexWebhookDeliveryEntity VALUES (1, 'retained-audio', X'010203', '{}', 'Success')")
            it.execSQL("INSERT INTO RecordingProcessingTaskEntity (created, attempts, status, recordingId, type) VALUES (1000, 1, 'Success', 1, 'AudioRecording')")
            it.execSQL("UPDATE room_master_table SET identity_hash = 'webhook-fixture' WHERE id = 42")
            it.execSQL("PRAGMA user_version = $version")
        }
    }

    @Test
    fun opensKnownWebhookVersionWithoutDroppingLegacyOrNativeData() = runBlocking<Unit> {
        val path = File(context.cacheDir, "migration-${UUID.randomUUID()}.db")
        try {
            fixture(path, 35)
            val db = open(path)
            try {
                assertEquals("retained-recording", db.localRecordingDao().getRecording(1)?.firestoreId)
                assertEquals("Private fixture", db.cachedItemDao().getById("retained-item")?.title)
            } finally {
                db.close()
            }
            BundledSQLiteDriver().open(path.absolutePath).use {
                assertEquals(34, it.scalar("PRAGMA user_version"))
                assertEquals(1, it.scalar("SELECT COUNT(*) FROM IndexWebhookDeliveryEntity WHERE status = 'Success'"))
                assertEquals(1, it.scalar("SELECT COUNT(*) FROM RecordingProcessingTaskEntity WHERE status = 'Success'"))
                it.prepare("SELECT audioData FROM IndexWebhookDeliveryEntity").use { row ->
                    check(row.step())
                    assertContentEquals(byteArrayOf(1, 2, 3), row.getBlob(0))
                }
            }
        } finally {
            context.deleteDatabase(path.absolutePath)
        }
    }

    @Test
    fun rejectsUnknownVersionOrChangedSchemaWithoutChangingDatabaseBytes() = runBlocking<Unit> {
        for (version in listOf(35, 36)) {
            val path = File(context.cacheDir, "migration-${UUID.randomUUID()}.db")
            try {
                fixture(path, version)
                if (version == 35) BundledSQLiteDriver().open(path.absolutePath).use {
                    it.execSQL("ALTER TABLE LocalRecording ADD COLUMN unexpected TEXT")
                }
                val before = path.readBytes()
                val db = open(path)
                try {
                    assertFails { db.localRecordingDao().getRecording(1) }
                } finally {
                    db.close()
                }
                assertContentEquals(before, path.readBytes())
                BundledSQLiteDriver().open(path.absolutePath).use {
                    assertEquals(version.toLong(), it.scalar("PRAGMA user_version"))
                    assertEquals(1, it.scalar("SELECT COUNT(*) FROM LocalRecording"))
                    assertEquals(1, it.scalar("SELECT COUNT(*) FROM IndexWebhookDeliveryEntity"))
                }
            } finally {
                context.deleteDatabase(path.absolutePath)
            }
        }
    }
}
