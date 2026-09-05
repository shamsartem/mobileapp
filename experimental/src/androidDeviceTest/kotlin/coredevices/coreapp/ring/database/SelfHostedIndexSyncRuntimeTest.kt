package coredevices.coreapp.ring.database

import androidx.room.Room
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.test.platform.app.InstrumentationRegistry
import com.russhwolf.settings.MapSettings
import com.russhwolf.settings.Settings
import coredevices.indexai.data.entity.AssistantSessionDocument
import coredevices.indexai.data.entity.ConversationMessageDocument
import coredevices.indexai.data.entity.ConversationMessageEntity
import coredevices.indexai.data.entity.ItemDocument
import coredevices.indexai.data.entity.LocalRecording
import coredevices.indexai.data.entity.MessageRole
import coredevices.indexai.data.entity.RecordingDocument
import coredevices.indexai.data.entity.RecordingEntry
import coredevices.indexai.data.entity.RecordingEntryEntity
import coredevices.indexai.data.entity.RecordingEntryStatus
import coredevices.libindex.database.entity.RingTransfer
import coredevices.libindex.database.entity.RingTransferStatus
import coredevices.ring.database.PreferencesImpl
import coredevices.ring.database.room.RingDatabase
import coredevices.ring.database.room.repository.ItemRepository
import coredevices.ring.database.room.repository.ListRepository
import coredevices.ring.selfhosted.api.SelfHostedIndexApi
import coredevices.ring.selfhosted.sync.IdentifiedDocument
import coredevices.ring.selfhosted.sync.MutationAcknowledgement
import coredevices.ring.selfhosted.sync.MutationKind
import coredevices.ring.selfhosted.sync.MutationOutcome
import coredevices.ring.selfhosted.sync.RecordingDeletionMutation
import coredevices.ring.selfhosted.sync.SelfHostedSyncState
import coredevices.ring.selfhosted.sync.SyncDelta
import coredevices.ring.selfhosted.sync.SyncDocuments
import coredevices.ring.selfhosted.sync.SyncRequest
import coredevices.ring.selfhosted.sync.SyncResponse
import coredevices.ring.service.RecordingBackgroundScope
import coredevices.ring.service.indexfeed.SelfHostedIndexSyncRuntime
import coredevices.util.integrations.IntegrationTokenStorage
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.engine.mock.toByteArray
import io.ktor.http.HttpHeaders
import io.ktor.http.headersOf
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Instant

class SelfHostedIndexSyncRuntimeTest {
    private lateinit var db: RingDatabase
    private lateinit var items: ItemRepository
    private lateinit var lists: ListRepository
    private lateinit var state: SelfHostedSyncState
    private lateinit var preferences: PreferencesImpl
    private lateinit var background: CoroutineScope
    private val clients = mutableListOf<SelfHostedIndexApi>()
    private val empty = SyncDocuments(emptyList(), emptyList(), emptyList())
    private val json = Json { encodeDefaults = true }
    private val timestamp = Instant.fromEpochMilliseconds(1000)

    @Before
    fun setUp() {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        db = Room.inMemoryDatabaseBuilder<RingDatabase>(context.applicationContext)
            .setDriver(BundledSQLiteDriver()).setQueryCoroutineContext(Dispatchers.IO).build()
        items = ItemRepository(db.cachedItemDao(), {})
        lists = ListRepository(db.cachedListDao())
        state = SelfHostedSyncState(MapSettings())
        preferences = PreferencesImpl(MapSettings())
        background = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    }

    @After
    fun tearDown() {
        background.cancel()
        clients.forEach { it.close() }
        db.close()
    }

    private fun runtime(handler: suspend (SyncRequest) -> SyncResponse): SelfHostedIndexSyncRuntime {
        val storage = object : IntegrationTokenStorage {
            override suspend fun saveToken(key: String, token: String) = Unit
            override suspend fun getToken(key: String) = "test-device-token"
            override suspend fun deleteToken(key: String) = Unit
        }
        val api = SelfHostedIndexApi("https://index.example", storage, MockEngine { request ->
            if (request.url.encodedPath == "/v1/events") {
                respond("event: revision\ndata: 0\n\n", headers = headersOf(HttpHeaders.ContentType, "text/event-stream"))
            } else {
                val response = handler(json.decodeFromString(request.body.toByteArray().decodeToString()))
                respond(json.encodeToString(response), headers = headersOf(HttpHeaders.ContentType, "application/json"))
            }
        }).also { clients += it }
        return SelfHostedIndexSyncRuntime(api, state, db, items, lists, preferences, RecordingBackgroundScope(background))
    }

    private fun accepted(request: SyncRequest, revision: Long = 1): SyncResponse {
        val acknowledgements = request.mutations.items.map { MutationKind.Item to it.id } +
            request.mutations.lists.map { MutationKind.List to it.id } +
            request.mutations.recordings.map { MutationKind.Recording to it.id } +
            request.recordingDeletions.map { MutationKind.RecordingDeletion to it.id }
        return SyncResponse(1, revision, revision, false, SyncDelta(empty, emptyList()),
            acknowledgements.map { (kind, id) -> MutationAcknowledgement(kind, id, MutationOutcome.Accepted) },
            SyncDelta(empty, emptyList()))
    }

    @Test
    fun uploadBatchesAreBoundedAndAcknowledgedRowsDoNotEcho() = runBlocking {
        items.writeBatch((0..500).map { "item-$it" to ItemDocument(title = "note $it", createdAt = timestamp, updatedAt = timestamp) })
        val sizes = mutableListOf<Int>()
        val runtime = runtime { request ->
            sizes += request.mutations.items.size
            accepted(request)
        }
        runtime.syncNow()
        assertEquals(listOf(500, 1), sizes)
        runtime.syncNow()
        assertEquals(listOf(500, 1, 0), sizes)
    }

    @Test
    fun remoteTombstoneWaitsForDirtyRecordingExcludedFromFullBatch() = runBlocking {
        items.writeBatch((0 until 500).map { "item-$it" to ItemDocument(title = "note", createdAt = timestamp, updatedAt = timestamp) })
        val recordingId = db.localRecordingDao().insertRecording(LocalRecording(
            firestoreId = "rec", updated = Instant.fromEpochMilliseconds(3000), lastPushedUpdated = 1000,
        ))
        db.recordingEntryDao().insertRecordingEntryRaw(RecordingEntryEntity(
            recordingId = recordingId, timestamp = timestamp, status = RecordingEntryStatus.completed, transcription = "offline edit",
        ))
        val requests = mutableListOf<SyncRequest>()
        val runtime = runtime { request ->
            requests += request
            if (requests.size == 1) {
                assertTrue(request.mutations.recordings.isEmpty())
                accepted(request).copy(changes = SyncDelta(empty, listOf("rec")))
            } else {
                assertEquals("offline edit", request.mutations.recordings.single().document.entries.single().transcription)
                // The server, after seeing updated=3000, accepts this newer edit.
                accepted(request, 2)
            }
        }
        runtime.syncNow()
        assertEquals(2, requests.size)
        assertEquals(3000, db.localRecordingDao().getRecording(recordingId)?.lastPushedUpdated)
        assertEquals("offline edit", db.recordingEntryDao().getEntriesForRecording(recordingId).first().single().transcription)
    }

    @Test
    fun committedPageSurvivesFailureAndRetryResumesItsCursor() = runBlocking<Unit> {
        val cursors = mutableListOf<Long>()
        var failSecondPage = true
        val runtime = runtime { request ->
            cursors += request.cursor
            if (request.cursor == 1L && failSecondPage) error("offline")
            val item = IdentifiedDocument("remote-${request.cursor}", ItemDocument(title = "page", createdAt = timestamp, updatedAt = timestamp))
            accepted(request, 2).copy(
                nextCursor = request.cursor + 1,
                hasMore = request.cursor == 0L,
                changes = SyncDelta(empty.copy(items = listOf(item)), emptyList()),
            )
        }
        assertFailsWith<IllegalStateException> { runtime.syncNow() }
        assertEquals(1, state.cursor)
        assertNotNull(items.getById("remote-0"))
        failSecondPage = false
        runtime.syncNow()
        assertEquals(listOf(0L, 1L, 1L), cursors)
        assertEquals(2, state.cursor)
        assertNotNull(items.getById("remote-1"))
    }

    @Test
    fun equalTimestampConcurrentEditSurvivesConflictAndIsSentNext() = runBlocking {
        val original = ItemDocument(title = "before", createdAt = timestamp, updatedAt = timestamp)
        items.setItem("note", original)
        val sent = mutableListOf<String>()
        val runtime = runtime { request ->
            sent += request.mutations.items.single().document.title
            if (sent.size == 1) {
                items.setItem("note", original.copy(title = "during request"))
                accepted(request).copy(
                    acknowledgements = listOf(MutationAcknowledgement(MutationKind.Item, "note", MutationOutcome.Conflict)),
                    reconciliation = SyncDelta(empty.copy(items = listOf(IdentifiedDocument("note", original.copy(title = "server winner")))), emptyList()),
                )
            } else accepted(request)
        }
        runtime.syncNow()
        assertEquals(listOf("before", "during request"), sent)
        assertEquals("during request", items.getById("note")?.title)
    }

    @Test
    fun malformedAcknowledgementCannotAdvanceCursorOrWriteRemoteRows() = runBlocking {
        items.setItem("note", ItemDocument(title = "local", createdAt = timestamp, updatedAt = timestamp))
        val runtime = runtime { request -> accepted(request).copy(
            acknowledgements = emptyList(),
            changes = SyncDelta(empty.copy(items = listOf(IdentifiedDocument("remote", ItemDocument(title = "remote")))), emptyList()),
        ) }
        assertFailsWith<IllegalArgumentException> { runtime.syncNow() }
        assertEquals(0, state.cursor)
        assertEquals(null, items.getById("remote"))
    }

    @Test
    fun pageReplayAfterCursorWriteFailureDoesNotDuplicateRecordingOrEntries() = runBlocking {
        val persisted = MapSettings()
        var failCursorWrite = true
        state = SelfHostedSyncState(object : Settings by persisted {
            override fun putLong(key: String, value: Long) {
                if (failCursorWrite) error("settings write failed")
                persisted.putLong(key, value)
            }
        })
        val remoteTimestamp = Instant.fromEpochSeconds(1, 123456789)
        val remote = RecordingDocument(timestamp = remoteTimestamp, updated = 2000, entries = listOf(
            RecordingEntry(timestamp = remoteTimestamp, status = RecordingEntryStatus.completed, transcription = "replayed"),
        ))
        val runtime = runtime { request -> accepted(request).copy(
            changes = SyncDelta(empty.copy(recordings = listOf(IdentifiedDocument("rec", remote))), emptyList()),
        ) }
        assertFailsWith<IllegalStateException> { runtime.syncNow() }
        val localId = assertNotNull(db.localRecordingDao().getByFirestoreId("rec")).id
        val entryId = db.recordingEntryDao().getEntriesForRecording(localId).first().single().id
        val transferId = db.ringTransferDao().insert(RingTransfer(
            recordingId = localId, recordingEntryId = entryId, isCurrentIndexIteration = true,
            transferInfo = null, status = RingTransferStatus.Completed,
        ))
        assertEquals(0, state.cursor)
        failCursorWrite = false
        runtime.syncNow()
        assertEquals(localId, db.localRecordingDao().getAllRecordings().first().single().id)
        assertEquals(entryId, db.recordingEntryDao().getEntriesForRecording(localId).first().single().id)
        assertEquals(entryId, db.ringTransferDao().getById(transferId)?.recordingEntryId)
        assertEquals(1, state.cursor)
    }

    @Test
    fun pendingEntryAndRingTransferKeepTheirIdsWhenServerCompletesRecording() = runBlocking {
        val recordingId = db.localRecordingDao().insertRecording(LocalRecording(firestoreId = "rec", updated = timestamp))
        val oldMessages = listOf(
            ConversationMessageDocument(MessageRole.user, "old user message"),
            ConversationMessageDocument(MessageRole.assistant, "assistant response"),
        )
        db.conversationMessageDao().insertMessagesRaw(oldMessages.map { ConversationMessageEntity(recordingId = recordingId, document = it) })
        val entryId = db.recordingEntryDao().insertRecordingEntryRaw(RecordingEntryEntity(
            recordingId = recordingId, timestamp = timestamp, fileName = "a.wav",
        ))
        val transferId = db.ringTransferDao().insert(RingTransfer(
            recordingId = recordingId, recordingEntryId = entryId, isCurrentIndexIteration = true,
            transferInfo = null, status = RingTransferStatus.Completed,
        ))
        val messages = listOf(oldMessages.first().copy(content = "edited user message"), oldMessages.last())
        val remote = RecordingDocument(timestamp = timestamp, updated = 2000, assistantSession = AssistantSessionDocument(title = "title", messages = messages), entries = listOf(
            RecordingEntry(timestamp = timestamp, fileName = "a.wav", status = RecordingEntryStatus.completed, transcription = "hello"),
        ))
        val runtime = runtime { request -> accepted(request).copy(
            acknowledgements = listOf(MutationAcknowledgement(MutationKind.Recording, "rec", MutationOutcome.RejectedOlder)),
            reconciliation = SyncDelta(empty.copy(recordings = listOf(IdentifiedDocument("rec", remote))), emptyList()),
        ) }
        runtime.syncNow()
        assertEquals(entryId, db.recordingEntryDao().getEntriesForRecording(recordingId).first().single().id)
        assertEquals(RecordingEntryStatus.completed, db.recordingEntryDao().getById(entryId)?.status)
        assertEquals(entryId, db.ringTransferDao().getById(transferId)?.recordingEntryId)
        assertEquals(2000, db.localRecordingDao().getRecording(recordingId)?.lastPushedUpdated)
        assertEquals(messages, db.conversationMessageDao().getMessagesForRecording(recordingId).first().map { it.document })
    }

    @Test
    fun rejectedRecordingDeletionRestoresAuthoritativeLiveDocumentBeforeRemovingMarker() = runBlocking {
        state.addRecordingDeletion(RecordingDeletionMutation("rec", 1000))
        val remote = RecordingDocument(timestamp = timestamp, updated = 2000, entries = listOf(
            RecordingEntry(timestamp = timestamp, transcription = "restore", status = RecordingEntryStatus.completed),
        ))
        val runtime = runtime { request -> accepted(request).copy(
            acknowledgements = listOf(MutationAcknowledgement(MutationKind.RecordingDeletion, "rec", MutationOutcome.RejectedOlder)),
            reconciliation = SyncDelta(empty.copy(recordings = listOf(IdentifiedDocument("rec", remote))), emptyList()),
        ) }
        runtime.syncNow()
        assertNotNull(db.localRecordingDao().getByFirestoreId("rec"))
        assertTrue(state.pendingRecordingDeletions.isEmpty())
        assertEquals(1, state.cursor)
    }

    @Test
    fun automaticSyncWaitsForBackupAndRetriesAfterOutageWithoutRestart() = runBlocking {
        preferences.setBackupEnabled(false)
        var calls = 0
        val synced = CompletableDeferred<Unit>()
        val runtime = runtime { request ->
            calls++
            if (calls == 1) error("offline")
            accepted(request).also { synced.complete(Unit) }
        }
        runtime.start()
        withTimeout(5_000) { while (lists.localCount() < 3) delay(10) }
        assertEquals(0, calls)
        preferences.setBackupEnabled(true)
        withTimeout(5_000) { synced.await() }
        assertTrue(calls >= 2)
    }
}
