package coredevices.coreapp.ring.queue

import androidx.room.Room
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.test.platform.app.InstrumentationRegistry
import com.russhwolf.settings.MapSettings
import coredevices.indexai.data.entity.ListDocument
import coredevices.indexai.data.entity.RecordingEntryStatus
import coredevices.indexai.data.entity.RingTransferInfo
import coredevices.libindex.database.entity.RingTransfer
import coredevices.libindex.database.entity.RingTransferStatus
import coredevices.libindex.database.repository.RingTransferRepository
import coredevices.ring.BuildKonfig
import coredevices.ring.agent.AgentFactory
import coredevices.ring.agent.BuiltinServletRepository
import coredevices.ring.agent.McpSessionFactory
import coredevices.ring.database.Preferences
import coredevices.ring.database.PreferencesImpl
import coredevices.ring.data.ProcessingTask
import coredevices.ring.data.RecordingProcessingTask
import coredevices.ring.database.firestore.dao.FirestoreRecordingsDao
import coredevices.ring.database.room.RingDatabase
import coredevices.ring.database.room.repository.ItemRepository
import coredevices.ring.database.room.repository.ListRepository
import coredevices.ring.database.room.repository.McpSandboxRepository
import coredevices.ring.database.room.repository.RecordingProcessingTaskRepository
import coredevices.ring.database.room.repository.RecordingRepository
import coredevices.ring.encryption.DocumentEncryptor
import coredevices.ring.encryption.EncryptionKeyManager
import coredevices.ring.external.indexwebhook.IndexWebhookApi
import coredevices.ring.external.indexwebhook.IndexWebhookPreferences
import coredevices.ring.external.indexwebhook.IndexWebhookRunResult
import coredevices.ring.selfhosted.api.SelfHostedIndexApi
import coredevices.ring.selfhosted.capture.SelfHostedAudioRecordingOperation
import coredevices.ring.selfhosted.capture.SelfHostedTextRecordingOperation
import coredevices.ring.selfhosted.sync.*
import coredevices.ring.service.RecordingBackgroundScope
import coredevices.ring.service.button.GestureRoutingPreferences
import coredevices.ring.service.button.RingGesture
import coredevices.ring.service.indexfeed.ItemFactory
import coredevices.ring.service.indexfeed.SelfHostedIndexSyncRuntime
import coredevices.ring.service.recordings.RecordingPreprocessor
import coredevices.ring.service.recordings.RecordingProcessingQueue
import coredevices.ring.service.recordings.button.RecordingOperationFactory
import coredevices.ring.storage.RealRecordingStorage
import coredevices.ring.storage.InvalidRecordingCaptureException
import coredevices.ring.storage.RecordingStorage
import coredevices.ring.storage.SelfHostedRecordingBlobStore
import coredevices.ring.util.trace.RingTraceSession
import coredevices.util.integrations.IntegrationTokenStorage
import coredevices.util.queue.TaskStatus
import coredevices.util.queue.PersistentQueueScheduler
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.engine.mock.toByteArray
import io.ktor.http.HttpHeaders
import io.ktor.http.headersOf
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import kotlinx.io.writeShortLe
import kotlinx.serialization.json.Json
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.koin.android.ext.koin.androidContext
import org.koin.core.context.startKoin
import org.koin.core.context.stopKoin
import org.koin.dsl.module
import java.io.File
import java.io.IOException
import java.util.UUID
import kotlin.test.*
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class SelfHostedCaptureTest {
    private val context = InstrumentationRegistry.getInstrumentation().targetContext
    private lateinit var db: RingDatabase
    private lateinit var items: ItemRepository
    private lateinit var lists: ListRepository
    private lateinit var preferences: PreferencesImpl
    private lateinit var storage: RecordingStorage
    private lateinit var api: SelfHostedIndexApi
    private lateinit var background: CoroutineScope
    private lateinit var queue: RecordingProcessingQueue
    private lateinit var factory: RecordingOperationFactory
    private lateinit var tasks: RecordingProcessingTaskRepository
    private lateinit var recordings: RecordingRepository
    private lateinit var transfers: RingTransferRepository
    private lateinit var trace: RingTraceSession
    private val json = Json { encodeDefaults = true }
    private val empty = SyncDocuments(emptyList(), emptyList(), emptyList())
    private val files = mutableListOf<String>()
    private val documents = mutableSetOf<String>()
    private val uploads = mutableListOf<Pair<String, ByteArray>>()
    @Volatile private var offline = true
    @Volatile private var loseProcessedResponse = false
    @Volatile private var unsupportedSyncVersion = false

    @Before
    fun setUp() {
        assertTrue(BuildKonfig.SELF_HOSTED_BACKEND_URL.isNotBlank(), "Build this suite with SELF_HOSTED_BACKEND_URL")
        stopKoin()
        db = Room.inMemoryDatabaseBuilder<RingDatabase>(context.applicationContext)
            .setDriver(BundledSQLiteDriver()).setQueryCoroutineContext(Dispatchers.IO).build()
        items = ItemRepository(db.cachedItemDao(), {})
        lists = ListRepository(db.cachedListDao())
        preferences = PreferencesImpl(MapSettings()).apply { setBackupEnabled(true) }
        val state = SelfHostedSyncState(MapSettings())
        background = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val tokenStorage = object : IntegrationTokenStorage {
            override suspend fun saveToken(key: String, token: String) = Unit
            override suspend fun getToken(key: String) = "device-token"
            override suspend fun deleteToken(key: String) = Unit
        }
        api = SelfHostedIndexApi("https://index.example", tokenStorage, MockEngine { request ->
            if (offline) throw IOException("offline")
            val path = request.url.encodedPath
            val body = if (path == "/v1/sync") {
                val sync = json.decodeFromString<SyncRequest>(request.body.toByteArray().decodeToString())
                documents += sync.mutations.recordings.map { it.id }
                val mutations = sync.mutations.items.map { MutationKind.Item to it.id } +
                    sync.mutations.lists.map { MutationKind.List to it.id } +
                    sync.mutations.recordings.map { MutationKind.Recording to it.id }
                json.encodeToString(SyncResponse(if (unsupportedSyncVersion) 2 else 1, 1, 1, false, SyncDelta(empty, emptyList()),
                    mutations.map { (kind, id) -> MutationAcknowledgement(kind, id, MutationOutcome.Accepted) },
                    SyncDelta(empty, emptyList())))
            } else {
                val segments = path.split('/')
                assertEquals("audio", segments[4])
                assertTrue(segments[3] in documents, "Recording must sync before audio")
                val id = segments[5]
                uploads += id to request.body.toByteArray()
                if (!id.endsWith("-original") && loseProcessedResponse) {
                    loseProcessedResponse = false
                    throw IOException("server stored audio but response was lost")
                }
                """{"blobId":"$id","sha256":"${"0".repeat(64)}"}"""
            }
            respond(body, headers = headersOf(HttpHeaders.ContentType, "application/json"))
        })
        val runtime = SelfHostedIndexSyncRuntime(api, state, db, items, lists, preferences, RecordingBackgroundScope(background))
        startKoin {
            androidContext(context)
            modules(module {
                single { db }
                single { items }
                single { lists }
                single { ItemFactory() }
                single<Preferences> { preferences }
                single { state }
                single { runtime }
                single<RecordingStorage> { storage }
                single { transfers }
            })
        }
        storage = RealRecordingStorage(db.cachedRecordingMetadataDao(), DocumentEncryptor(EncryptionKeyManager(context)),
            preferences, SelfHostedRecordingBlobStore(api))
        tasks = RecordingProcessingTaskRepository(db.recordingProcessingTaskDao())
        transfers = RingTransferRepository(db.ringTransferDao(), db)
        recordings = RecordingRepository(db.localRecordingDao(), db.recordingEntryDao(),
            FirestoreRecordingsDao { error("Pebble Firestore must not be used") }, RecordingBackgroundScope(background), db, state)
        val sandbox = McpSandboxRepository(db.mcpSandboxGroupDao(), db.builtinMcpGroupAssociationDao(),
            db.httpMcpServerDao(), db.httpMcpGroupAssociationDao(), FakeServletRepository(), db)
        trace = RingTraceSession(db.traceEntryDao(), db.traceSessionDao())
        factory = RecordingOperationFactory(AgentFactory(), sandbox, McpSessionFactory(sandbox, BuiltinServletRepository()),
            GestureRoutingPreferences(MapSettings(), preferences), object : IndexWebhookApi {
                override fun uploadIfEnabled(samples: ShortArray?, sampleRate: Int, recordingId: String,
                    transcription: String?, recordedAt: Instant, gesture: RingGesture) = error("Webhook must not run")
                override suspend fun sendTestEvent(gesture: RingGesture, url: String, headers: Map<String, String>): IndexWebhookRunResult =
                    error("Webhook must not run")
            }, IndexWebhookPreferences(MapSettings()), storage, trace, ItemFactory(), items)
        queue = newQueue()
    }

    private fun newQueue() = RecordingProcessingQueue(storage, transfers, recordings, tasks, factory,
        RecordingBackgroundScope(background), RecordingPreprocessor(storage), trace, rescheduleDelay = 50.milliseconds)

    @After
    fun tearDown() {
        queue.close()
        background.cancel()
        api.close()
        db.close()
        stopKoin()
        files.forEach { id ->
            listOf("$id.pcm", "$id-original.pcm", id, "$id-original").forEach {
                File(context.filesDir, "recording-pcm/$it").delete()
            }
            listOf("$id.m4a", "$id-original.m4a").forEach { File(context.filesDir, "recordings/$it").delete() }
        }
    }

    private suspend fun audio(): String {
        val id = "capture-test-${UUID.randomUUID()}".also { files += it }
        storage.openOriginalRecordingSink(id, 16000, "audio/raw").use { sink ->
            repeat(1600) { sink.writeShortLe((it % 100 * 100).toShort()) }
        }
        return id
    }

    private suspend fun awaitTask(id: Long, predicate: (coredevices.ring.data.entity.room.RecordingProcessingTaskEntity) -> Boolean) =
        withTimeout(30.seconds) { db.recordingProcessingTaskDao().getTaskByIdFlow(id).first { it != null && predicate(it) }!! }

    @Test
    fun startupReplayAndFreshScheduleCannotRunTheSameTaskConcurrently() = runBlocking<Unit> {
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val scheduler = object : PersistentQueueScheduler<RecordingProcessingTask>(tasks, background, "duplicate-test", maxConcurrency = 2) {
            override suspend fun processTask(task: RecordingProcessingTask) {
                entered.complete(Unit)
                release.await()
            }
            fun enqueue(id: Long) = scheduleTask(id)
        }
        try {
            val id = tasks.insertTask(RecordingProcessingTask(task = ProcessingTask.TextRecording("note")))
            scheduler.resumePendingTasks()
            withTimeout(5.seconds) { entered.await() }
            scheduler.enqueue(id)
            delay(200)
            assertEquals(1, tasks.getTaskById(id)?.attempts)
            release.complete(Unit)
            awaitTask(id) { it.status == TaskStatus.Success }
            withTimeout(5.seconds) { scheduler.activeTaskIds.first { it.isEmpty() } }
        } finally {
            scheduler.close()
        }
    }

    @Test
    fun offlinePhoneCaptureSurvivesAttemptLimitAndRestartThenReplaysIdenticalUploads() = runBlocking {
        val file = audio()
        queue.queueLocalAudioProcessing(file)
        val pending = awaitTask(1) { it.attempts >= 4 }
        assertEquals(TaskStatus.Pending, pending.status)
        val recording = db.localRecordingDao().getAllRecordings().first().single()
        val entry = db.recordingEntryDao().getEntriesForRecording(recording.id).first().single()
        assertEquals(RecordingEntryStatus.pending, entry.status)
        assertNotNull(recording.firestoreId)
        assertEquals(recording.id, pending.recordingId)
        assertTrue(File(context.filesDir, "recording-pcm").exists())
        queue.close()
        background.cancel()
        background = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        offline = false
        unsupportedSyncVersion = true
        loseProcessedResponse = true
        queue = newQueue()
        queue.resumePendingTasks()
        val protocolFailure = awaitTask(1) { it.attempts >= pending.attempts + 4 }
        assertEquals(TaskStatus.Pending, protocolFailure.status)
        assertEquals(RecordingEntryStatus.pending, db.recordingEntryDao().getById(entry.id)?.status)
        unsupportedSyncVersion = false
        awaitTask(1) { it.status == TaskStatus.Success }
        assertEquals(listOf("$file-original", file, "$file-original", file), uploads.map { it.first })
        assertContentEquals(uploads[0].second, uploads[2].second)
        assertContentEquals(uploads[1].second, uploads[3].second)
        assertEquals(recording.id, db.localRecordingDao().getAllRecordings().first().single().id)
        assertEquals(entry.id, db.recordingEntryDao().getEntriesForRecording(recording.id).first().single().id)
    }

    @Test
    fun ringCaptureLinksNativePendingEntryAndCompletedReplayDoesNotUpload() = runBlocking {
        val file = audio()
        val transferInfo = RingTransferInfo(collectionStartIndex = 42, buttonPressed = 1000)
        val transfer = db.ringTransferDao().insert(RingTransfer(recordingId = null, recordingEntryId = null,
            isCurrentIndexIteration = true, transferInfo = transferInfo, status = RingTransferStatus.Completed, fileId = file))
        preferences.setBackupEnabled(false)
        queue.queueAudioProcessing(transfer, null)
        awaitTask(1) { it.attempts >= 2 }
        queue.close()
        val linked = assertNotNull(transfers.getRingTransferById(transfer))
        val entryId = assertNotNull(linked.recordingEntryId)
        val recordingId = assertNotNull(linked.recordingId)
        assertEquals(transferInfo, db.recordingEntryDao().getById(entryId)?.ringTransferInfo)
        db.recordingEntryDao().updateRecordingEntryStatus(entryId, RecordingEntryStatus.completed)
        SelfHostedAudioRecordingOperation(recordingId, file, transfer).run(null)
        assertEquals(RecordingEntryStatus.completed, db.recordingEntryDao().getById(entryId)?.status)
        assertTrue(uploads.isEmpty())
        assertEquals(1, db.recordingEntryDao().getEntriesForRecording(recordingId).first().size)
        db.recordingEntryDao().updateRecordingEntryStatus(entryId, RecordingEntryStatus.transcription_error)
        SelfHostedAudioRecordingOperation(recordingId, file, transfer).run(null)
        assertEquals(RecordingEntryStatus.transcription_error, db.recordingEntryDao().getById(entryId)?.status)
        assertTrue(uploads.isEmpty())
    }

    @Test
    fun missingLocalCaptureIsVisibleAsErrorInsteadOfRetryingForever() = runBlocking {
        offline = false
        val recording = recordings.createRecording("missing-capture")
        assertFailsWith<InvalidRecordingCaptureException> {
            SelfHostedAudioRecordingOperation(recording, "missing-file", null).run(null)
        }
        val entry = db.recordingEntryDao().getEntriesForRecording(recording).first().single()
        assertEquals(RecordingEntryStatus.transcription_error, entry.status)
        assertEquals("Recording could not be uploaded", entry.error)
    }

    @Test
    fun typedNoteIsOfflineAndReplayingPreservesEditedAndDeletedItem() = runBlocking {
        lists.setList("work", ListDocument(title = "Work", createdAt = Instant.fromEpochMilliseconds(1)))
        queue.queueTextProcessing("Work: buy pens")
        awaitTask(1) { it.status == TaskStatus.Success }
        val recording = db.localRecordingDao().getAllRecordings().first().single()
        val note = items.getAllFlow().first().single()
        assertEquals("buy pens", note.title)
        assertEquals(listOf("work"), note.parentListIds())
        assertEquals(recording.firestoreId, note.sourceRecordingId)
        assertEquals("Work: buy pens", db.recordingEntryDao().getEntriesForRecording(recording.id).first().single().transcription)
        items.setItem(note.firestoreId, note.toDocument().copy(title = "edited"))
        val operation = SelfHostedTextRecordingOperation(recording.id, "Work: buy pens")
        operation.run(null)
        assertEquals("edited", items.getById(note.firestoreId)?.title)
        items.softDelete(note.firestoreId)
        operation.run(null)
        assertTrue(assertNotNull(items.getById(note.firestoreId)).deleted)
        assertEquals(1, db.recordingEntryDao().getEntriesForRecording(recording.id).first().size)
        assertTrue(documents.isEmpty())
        assertTrue(uploads.isEmpty())
    }
}
