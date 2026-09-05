package coredevices.ring.service.indexfeed

import androidx.room.Transactor
import androidx.room.useWriterConnection
import co.touchlab.kermit.Logger
import coredevices.indexai.data.entity.ConversationMessageEntity
import coredevices.indexai.data.entity.LocalRecording
import coredevices.indexai.data.entity.RecordingDocument
import coredevices.indexai.data.entity.RecordingEntry
import coredevices.indexai.data.entity.RecordingEntryEntity
import coredevices.indexai.data.entity.RecordingEntryStatus
import coredevices.ring.data.entity.room.indexfeed.CachedItem
import coredevices.ring.data.entity.room.indexfeed.CachedList
import coredevices.ring.database.Preferences
import coredevices.ring.database.room.RingDatabase
import coredevices.ring.database.room.repository.ItemRepository
import coredevices.ring.database.room.repository.ListRepository
import coredevices.ring.selfhosted.api.SelfHostedIndexApi
import coredevices.ring.selfhosted.api.SelfHostedSyncResponseTooLargeException
import coredevices.ring.selfhosted.sync.IdentifiedDocument
import coredevices.ring.selfhosted.sync.MutationKind
import coredevices.ring.selfhosted.sync.MutationOutcome
import coredevices.ring.selfhosted.sync.SELF_HOSTED_MAXIMUM_SYNC_BYTES
import coredevices.ring.selfhosted.sync.SELF_HOSTED_SYNC_PROTOCOL_VERSION
import coredevices.ring.selfhosted.sync.SelfHostedSyncState
import coredevices.ring.selfhosted.sync.SyncDelta
import coredevices.ring.selfhosted.sync.SyncDocuments
import coredevices.ring.selfhosted.sync.SyncRequest
import coredevices.ring.selfhosted.sync.SyncResponse
import coredevices.ring.selfhosted.sync.encodedSize
import coredevices.ring.service.RecordingBackgroundScope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Instant

class SelfHostedIndexSyncRuntime(
    private val api: SelfHostedIndexApi,
    private val state: SelfHostedSyncState,
    private val db: RingDatabase,
    private val itemRepo: ItemRepository,
    private val listRepo: ListRepository,
    private val preferences: Preferences,
    private val scope: RecordingBackgroundScope,
) : IndexSyncRuntime {
    private val log = Logger.withTag("SelfHostedIndexSync")
    private val mutex = Mutex()
    // Keep the existing cached values: timestamps alone miss two edits in the
    // same millisecond (Room truncates Instant) and can suppress an unsent edit.
    private val itemLastApplied = mutableMapOf<String, CachedItem>()
    private val listLastApplied = mutableMapOf<String, CachedList>()
    private var job: Job? = null

    override fun start() {
        if (job != null) return
        job = scope.launch(Dispatchers.IO) {
            // Default lists remain usable before enrollment and while offline.
            transaction {
                DefaultListsBootstrap.documents(Instant.fromEpochMilliseconds(0)).forEach { (id, doc) ->
                    if (listRepo.getById(id) == null) listRepo.upsertLocal(id, doc)
                }
            }
            api.restoreAuthentication()
            combine(api.authenticated, preferences.backupEnabled) { authenticated, backup ->
                authenticated && backup
            }.collectLatest { enabled ->
                if (enabled) automaticSync()
            }
        }
    }

    private suspend fun automaticSync() = coroutineScope {
        val triggers = Channel<Unit>(Channel.CONFLATED)
        launch {
            combine(
                itemRepo.getAllForSyncFlow(), listRepo.getAllForSyncFlow(),
                db.localRecordingDao().getAllRecordings(),
            ) { _, _, _ -> Unit }.collect { triggers.trySend(Unit) }
        }
        launch {
            var retryMs = 1_000L
            while (isActive) {
                try {
                    api.revisions().collect { revision ->
                        retryMs = 1_000L
                        if (revision > state.cursor) triggers.trySend(Unit)
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    log.w(e) { "Revision connection interrupted; reconnecting" }
                }
                delay(retryMs)
                retryMs = (retryMs * 2).coerceAtMost(30_000L)
            }
        }
        triggers.trySend(Unit)
        for (trigger in triggers) {
            var retryMs = 1_000L
            while (isActive) {
                try {
                    syncNow()
                    break
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    log.w(e) { "Sync failed; local changes retained for retry" }
                    delay(retryMs)
                    retryMs = (retryMs * 2).coerceAtMost(30_000L)
                }
            }
        }
    }

    override suspend fun onBackgroundSync() {
        if (!preferences.backupEnabled.value) return
        try {
            syncNow()
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.w(e) { "Background sync failed; local changes retained for retry" }
        }
    }

    /** Explicit sync is also available when automatic backup is disabled. */
    suspend fun syncNow() = mutex.withLock {
        var shouldPull = true
        var mutationLimit = 500
        while (true) {
            val snapshot = snapshot()
            var request = snapshot.request(mutationLimit)
            while (request.encodedSize() > SELF_HOSTED_MAXIMUM_SYNC_BYTES && request.mutationCount > 1) {
                mutationLimit = request.mutationCount / 2
                request = snapshot.request(mutationLimit)
            }
            require(request.encodedSize() <= SELF_HOSTED_MAXIMUM_SYNC_BYTES) {
                "A sync mutation exceeds the 4 MiB protocol limit"
            }
            if (!shouldPull && request.mutations.items.isEmpty() && request.mutations.lists.isEmpty() &&
                request.mutations.recordings.isEmpty() && request.recordingDeletions.isEmpty()) break
            val response = try {
                api.sync(request)
            } catch (e: SelfHostedSyncResponseTooLargeException) {
                if (request.mutationCount <= 1) throw e
                mutationLimit = request.mutationCount / 2
                continue
            }
            mutationLimit = 500
            requireMatchingAcknowledgements(request, response)
            val delta = SyncDelta(
                SyncDocuments(
                    (response.changes.documents.recordings + response.reconciliation.documents.recordings).associateBy { it.id }.values.toList(),
                    (response.changes.documents.items + response.reconciliation.documents.items).associateBy { it.id }.values.toList(),
                    (response.changes.documents.lists + response.reconciliation.documents.lists).associateBy { it.id }.values.toList(),
                ),
                (response.changes.removedRecordingIds + response.reconciliation.removedRecordingIds).distinct(),
            )
            val appliedItems = mutableMapOf<String, CachedItem>()
            val appliedLists = mutableMapOf<String, CachedList>()
            val restoredDeletionIds = request.recordingDeletions.filter { deletion ->
                deletion in state.pendingRecordingDeletions &&
                    response.reconciliation.documents.recordings.any { it.id == deletion.id }
            }.map { it.id }.toSet()
            val arbitratedRecordingIds = (request.mutations.recordings.map { it.id } +
                request.recordingDeletions.map { it.id }).toSet()
            transaction {
                apply(delta, snapshot, restoredDeletionIds, arbitratedRecordingIds, appliedItems, appliedLists)
                for (sent in request.mutations.items) {
                    val local = itemRepo.getById(sent.id)
                    if (local != null && local.toDocument() == sent.document) appliedItems[sent.id] = local
                }
                for (sent in request.mutations.lists) {
                    val local = listRepo.getById(sent.id)
                    if (local != null && local.toDocument() == sent.document) appliedLists[sent.id] = local
                }
                for (sent in request.mutations.recordings) {
                    val ack = response.acknowledgements.single {
                        it.kind == MutationKind.Recording && it.id == sent.id
                    }
                    if (ack.outcome == MutationOutcome.Accepted || ack.outcome == MutationOutcome.AlreadyCurrent) {
                        db.localRecordingDao().getByFirestoreId(sent.id)?.let { local ->
                            if (recordingDocument(local) == sent.document) {
                                db.localRecordingDao().setLastPushedUpdated(
                                    local.id, maxOf(local.lastPushedUpdated ?: 0, sent.document.updated),
                                )
                            }
                        }
                    }
                }
            }
            // These are committed only after all Room writes succeed. A crash before
            // this point replays the page; it cannot skip uncommitted remote changes.
            itemLastApplied.putAll(appliedItems)
            listLastApplied.putAll(appliedLists)
            for (deletion in request.recordingDeletions) {
                state.acknowledgeRecordingDeletion(deletion)
            }
            state.advanceCursor(response.nextCursor)
            shouldPull = response.hasMore
        }
    }

    private data class Snapshot(
        val items: Map<String, CachedItem>,
        val lists: Map<String, CachedList>,
        val recordings: Map<String, Pair<LocalRecording, RecordingDocument>>,
    )

    private suspend fun snapshot(): Snapshot = transaction {
        Snapshot(
            itemRepo.getAllForSyncFlow().first().associateBy { it.firestoreId },
            listRepo.getAllForSyncFlow().first().associateBy { it.firestoreId },
            db.localRecordingDao().getAllRecordings().first().mapNotNull { local ->
                val id = local.firestoreId ?: return@mapNotNull null
                id to (local to recordingDocument(local))
            }.toMap(),
        )
    }

    private suspend fun recordingDocument(local: LocalRecording) = local.toDocument(
        db.recordingEntryDao().getEntriesForRecording(local.id).first().map { it.toSyncDocument() },
        db.conversationMessageDao().getMessagesForRecording(local.id).first().map { it.document },
    )

    private val SyncRequest.mutationCount: Int
        get() = recordingDeletions.size + mutations.recordings.size + mutations.items.size + mutations.lists.size

    private fun Snapshot.request(limit: Int = 500): SyncRequest {
        val deletions = state.pendingRecordingDeletions.take(limit)
        var available = limit - deletions.size
        val lists = lists.values.filter {
            (!it.locked || it.deleted) && listLastApplied[it.firestoreId] != it
        }.take(available).map { IdentifiedDocument(it.firestoreId, it.toDocument()) }
        available -= lists.size
        val items = items.values.filter {
            (!it.locked || it.deleted) && itemLastApplied[it.firestoreId] != it
        }.take(available).map { IdentifiedDocument(it.firestoreId, it.toDocument()) }
        available -= items.size
        val pendingIds = state.pendingRecordingDeletions.map { it.id }.toSet()
        val recordings = recordings.filter { (id, value) ->
            val (local, doc) = value
            id !in pendingIds && doc.entries.isNotEmpty() &&
                (local.lastPushedUpdated?.let { doc.updated > it } ?: true)
        }.entries.take(available).map { IdentifiedDocument(it.key, it.value.second) }
        return SyncRequest(SELF_HOSTED_SYNC_PROTOCOL_VERSION, state.cursor, SyncDocuments(recordings, items, lists), deletions)
    }

    private suspend fun apply(
        delta: SyncDelta,
        before: Snapshot,
        restoredDeletionIds: Set<String>,
        arbitratedRecordingIds: Set<String>,
        appliedItems: MutableMap<String, CachedItem>,
        appliedLists: MutableMap<String, CachedList>,
    ) {
        for ((id, remote) in delta.documents.items) {
            val local = itemRepo.getById(id)
            if (local == before.items[id] && (local == null || remote.updatedAt >= local.updatedAt)) {
                itemRepo.upsertLocal(id, remote)
                appliedItems[id] = requireNotNull(itemRepo.getById(id))
            }
        }
        for ((id, remote) in delta.documents.lists) {
            val local = listRepo.getById(id)
            if (local == before.lists[id] && (local == null || remote.updatedAt >= local.updatedAt)) {
                listRepo.upsertLocal(id, remote)
                appliedLists[id] = requireNotNull(listRepo.getById(id))
            }
        }
        for ((id, remote) in delta.documents.recordings) {
            if (id !in restoredDeletionIds && state.pendingRecordingDeletions.any { it.id == id }) continue
            val local = db.localRecordingDao().getByFirestoreId(id)
            if (local == before.recordings[id]?.first &&
                (local == null || recordingDocument(local) == before.recordings[id]?.second) &&
                (local == null || remote.updated >= local.updated.toEpochMilliseconds())) {
                mergeRecording(id, local, remote)
            }
        }
        for (id in delta.removedRecordingIds) {
            val local = db.localRecordingDao().getByFirestoreId(id) ?: continue
            // A tombstone carries no timestamp. An unsent dirty recording may
            // be newer; let the server arbitrate its upsert in the next batch.
            val dirty = local.lastPushedUpdated?.let { local.updated.toEpochMilliseconds() > it } ?: true
            if (dirty && id !in arbitratedRecordingIds) continue
            if (local == before.recordings[id]?.first && recordingDocument(local) == before.recordings[id]?.second) {
                db.localRecordingDao().deleteRecording(local)
            }
        }
    }

    private suspend fun mergeRecording(id: String, local: LocalRecording?, remote: RecordingDocument) {
        val row = (local ?: LocalRecording(firestoreId = id)).copy(
            localTimestamp = remote.timestamp,
            assistantTitle = remote.assistantSession?.title,
            updated = Instant.fromEpochMilliseconds(remote.updated),
            lastPushedUpdated = remote.updated,
        )
        val localId = local?.id ?: db.localRecordingDao().insertRecording(row)
        val oldEntries = db.recordingEntryDao().getEntriesForRecording(localId).first()
        val mergedEntries = mergeRecordingEntries(localId, oldEntries, remote.entries)
        val oldMessages = db.conversationMessageDao().getMessagesForRecording(localId).first()
        val messages = remote.assistantSession?.messages.orEmpty()
        val preservedMessages = oldMessages.zip(messages).takeWhile { (old, remote) -> old.document == remote }.map { it.first }
        if (oldMessages.map { it.document } != messages) {
            db.conversationMessageDao().deleteAllForRecording(localId)
            db.conversationMessageDao().insertMessagesRaw(messages.mapIndexed { index, message ->
                // Only retain a matching prefix: messages are ordered by local ID.
                preservedMessages.getOrNull(index)
                    ?: ConversationMessageEntity(recordingId = localId, document = message)
            })
        }
        // UPDATE preserves foreign-key links from RingTransfer and queue tasks.
        // Deleting then reinserting the same ID would still clear those links.
        db.recordingEntryDao().deleteByIds(oldEntries.map { it.id } - mergedEntries.map { it.id }.toSet())
        mergedEntries.forEach { entry ->
            val linked = entry.copy(userMessageId = entry.userMessageId?.takeIf { id -> preservedMessages.any { it.id == id } })
            if (linked.id == 0L) db.recordingEntryDao().insertRecordingEntryRaw(linked)
            else db.recordingEntryDao().update(linked)
        }
        val hasUnsentPending = mergedEntries.any { entry ->
            remote.entries.none { it.matches(entry) }
        }
        db.localRecordingDao().updateRecording(row.copy(
            id = localId,
            updated = if (hasUnsentPending) maxOf(Clock.System.now(), row.updated + 1.milliseconds) else row.updated,
        ))
    }

    private suspend fun <T> transaction(block: suspend () -> T): T = db.useWriterConnection {
        it.withTransaction(Transactor.SQLiteTransactionType.IMMEDIATE) { block() }
    }
}

internal fun requireMatchingAcknowledgements(request: SyncRequest, response: SyncResponse) {
    val expected = request.mutations.recordings.map { MutationKind.Recording to it.id } +
        request.mutations.items.map { MutationKind.Item to it.id } +
        request.mutations.lists.map { MutationKind.List to it.id } +
        request.recordingDeletions.map { MutationKind.RecordingDeletion to it.id }
    val actual = response.acknowledgements.map { it.kind to it.id }
    require(actual.size == expected.size && actual.toSet() == expected.toSet()) { "Mismatched sync acknowledgements" }
    for (ack in response.acknowledgements) {
        if (ack.outcome == MutationOutcome.Accepted) continue
        val reconciled = when (ack.kind) {
            MutationKind.Item -> response.reconciliation.documents.items.any { it.id == ack.id }
            MutationKind.List -> response.reconciliation.documents.lists.any { it.id == ack.id }
            MutationKind.Recording, MutationKind.RecordingDeletion ->
                response.reconciliation.documents.recordings.any { it.id == ack.id } ||
                    ack.id in response.reconciliation.removedRecordingIds
        }
        require(reconciled) { "Missing authoritative reconciliation for ${ack.kind}/${ack.id}" }
    }
}

internal fun RecordingEntryEntity.toSyncDocument() = RecordingEntry(
    timestamp, fileName, status, transcription, transcribedUsingModel, error, errorType, ringTransferInfo, userMessageId,
)

internal fun mergeRecordingEntries(
    recordingId: Long,
    local: List<RecordingEntryEntity>,
    remote: List<RecordingEntry>,
): List<RecordingEntryEntity> {
    val remaining = local.toMutableList()
    val merged = remote.map { entry ->
        val existing = remaining.firstOrNull { entry.matches(it) }
        remaining.remove(existing)
        RecordingEntryEntity(
            id = existing?.id ?: 0, recordingId = recordingId, timestamp = entry.timestamp,
            fileName = entry.fileName, status = entry.status, transcription = entry.transcription,
            transcribedUsingModel = entry.transcribedUsingModel, error = entry.error, errorType = entry.errorType,
            ringTransferInfo = entry.ringTransferInfo, userMessageId = existing?.userMessageId,
        )
    }
    return merged + remaining.filter { it.status == RecordingEntryStatus.pending || it.status == RecordingEntryStatus.agent_processing }
}

// Room stores milliseconds even when a remote Instant includes nanoseconds.
private fun RecordingEntry.matches(local: RecordingEntryEntity) =
    timestamp.toEpochMilliseconds() == local.timestamp.toEpochMilliseconds() && fileName == local.fileName
