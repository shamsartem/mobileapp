package coredevices.coreapp.ring

import android.content.Context
import androidx.test.platform.app.InstrumentationRegistry
import com.russhwolf.settings.MapSettings
import coredevices.ring.data.entity.room.CachedRecordingMetadata
import coredevices.ring.database.PreferencesImpl
import coredevices.ring.database.room.dao.CachedRecordingMetadataDao
import coredevices.ring.encryption.DocumentEncryptor
import coredevices.ring.encryption.EncryptionKeyManager
import coredevices.ring.storage.RealRecordingStorage
import coredevices.ring.storage.RecordingBlob
import coredevices.ring.storage.RecordingBlobStore
import kotlinx.coroutines.runBlocking
import kotlinx.io.writeShortLe
import org.junit.Test
import org.koin.core.context.startKoin
import org.koin.core.context.stopKoin
import org.koin.dsl.module
import java.io.File
import java.util.UUID
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class RecordingStorageTransportTest {
    @Test
    fun originalUploadsFirstAndLostResponseRetriesIdenticalEncodedBytes() = runBlocking<Unit> {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val id = "transport-test-${UUID.randomUUID()}"
        val rows = mutableMapOf<String, CachedRecordingMetadata>()
        val metadata = object : CachedRecordingMetadataDao {
            override suspend fun insert(metadata: CachedRecordingMetadata): Long = insertOrReplace(metadata)
            override suspend fun insertOrReplace(metadata: CachedRecordingMetadata): Long {
                rows[metadata.id] = metadata
                return 1
            }
            override suspend fun get(id: String) = rows[id]
            override suspend fun deleteAll() = rows.clear()
        }
        val uploads = mutableListOf<Pair<String, ByteArray>>()
        val transport = object : RecordingBlobStore {
            override suspend fun get(id: String): RecordingBlob = error("All audio is local")
            override suspend fun put(id: String, bytes: ByteArray, metadata: Map<String, String>, recordingId: String?) {
                assertEquals("parent", recordingId)
                assertEquals(mapOf("sampleRate" to "16000"), metadata)
                uploads += id to bytes
                if (uploads.size == 2) error("Response lost after accepting processed audio")
            }
        }
        stopKoin()
        startKoin { modules(module { single<Context> { context } }) }
        try {
            val storage = RealRecordingStorage(metadata, DocumentEncryptor(EncryptionKeyManager(context)), PreferencesImpl(MapSettings()), transport)
            storage.openOriginalRecordingSink(id, 16000, "audio/raw").use { sink ->
                repeat(1600) { sink.writeShortLe(42) }
            }
            storage.openRecordingSink(id, 16000, "audio/raw").use { sink ->
                repeat(1600) { sink.writeShortLe(24) }
            }
            assertFailsWith<IllegalStateException> { storage.persistRecording(id, "parent") }
            storage.openRecordingSink(id, 16000, "audio/raw").use { sink ->
                repeat(1600) { sink.writeShortLe(99) }
            }
            storage.persistRecording(id, "parent")
            assertEquals(listOf("$id-original", id, "$id-original", id), uploads.map { it.first })
            assertTrue(uploads.first().second.isNotEmpty())
            assertContentEquals(uploads[0].second, uploads[2].second)
            assertContentEquals(uploads[1].second, uploads[3].second)
        } finally {
            for (variant in listOf(id, "$id-original")) {
                File(context.cacheDir, "recordings/$variant").delete()
                File(context.filesDir, "recording-pcm/$variant").delete()
                File(context.filesDir, "recordings/$variant.m4a").delete()
                File(context.filesDir, "recordings/$variant.m4a.tmp").delete()
            }
            stopKoin()
        }
    }
}
