@file:OptIn(kotlin.time.ExperimentalTime::class)

package coredevices.ring.service.indexfeed

import co.touchlab.kermit.Logger
import coredevices.indexai.data.entity.ConversationMessageEntity
import coredevices.indexai.data.entity.RecordingDocument
import coredevices.indexai.data.entity.RecordingEntry
import coredevices.indexai.data.entity.RecordingEntryEntity
import coredevices.indexai.data.entity.RecordingEntryStatus
import coredevices.indexai.database.dao.ConversationMessageDao
import coredevices.indexai.database.dao.RecordingEntryDao
import coredevices.ring.database.Preferences
import coredevices.ring.database.firestore.dao.FirestoreRecordingsDao
import coredevices.ring.database.firestore.dao.FirestoreTracesDao
import coredevices.ring.database.room.repository.RecordingRepository
import coredevices.ring.encryption.DocumentEncryptor
import coredevices.ring.service.RecordingBackgroundScope
import coredevices.ring.util.trace.TraceSessionExporter
import dev.gitlive.firebase.Firebase
import dev.gitlive.firebase.auth.auth
import dev.gitlive.firebase.firestore.QuerySnapshot
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.IO
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.koin.core.component.KoinComponent
import org.koin.core.component.get
import kotlin.time.Clock
import kotlin.time.Instant

class FirestoreIndexSyncRuntime(
    private val defaultListsBootstrap: DefaultListsBootstrap,
    private val recordingRepository: RecordingRepository,
    private val scope: RecordingBackgroundScope,
) : IndexSyncRuntime, KoinComponent {
    private val logger = Logger.withTag("FirestoreIndexSync")

    private val uploadingIds = mutableSetOf<Long>()
    private val uploadingIdsLock = Mutex()

    private suspend fun inFlightSnapshot(): Set<Long> =
        uploadingIdsLock.withLock { uploadingIds.toSet() }

    private suspend fun tryBeginUpload(id: Long): Boolean =
        uploadingIdsLock.withLock { uploadingIds.add(id) }

    private suspend fun finishUpload(id: Long) = withContext(NonCancellable) {
        uploadingIdsLock.withLock { uploadingIds.remove(id) }
    }

    override fun start() {
        get<IndexFeedSyncService>()
        val preferences: Preferences = get()
        recordingRepository.getAllRecordings().drop(1).debounce(2000).onEach { recordings ->
            if (!preferences.backupEnabled.value) return@onEach
            val firestoreRecordingsDao: FirestoreRecordingsDao = get()
            val recordingEntryDao: RecordingEntryDao = get()
            val conversationMessageDao: ConversationMessageDao = get()

            val recordingsWithEntries = recordingEntryDao
                .getRecordingIdsWithEntries()
                .toHashSet()

            val inFlight = inFlightSnapshot()
            val needsPush = recordings.filter { localRecording ->
                if (localRecording.id in inFlight) return@filter false
                if (localRecording.id !in recordingsWithEntries) return@filter false
                val watermark = localRecording.lastPushedUpdated
                watermark == null || localRecording.updated.toEpochMilliseconds() > watermark
            }
            if (needsPush.isEmpty()) return@onEach
            logger.i { "Found ${needsPush.size} local recordings to push" }

            for (localRecording in needsPush) {
                if (!tryBeginUpload(localRecording.id)) continue
                scope.launch(Dispatchers.IO) {
                    try {
                        val entries = recordingEntryDao.getEntriesForRecording(localRecording.id).first()
                        val messages = conversationMessageDao.getMessagesForRecording(localRecording.id).first()
                        var doc = localRecording.toDocument(
                            entries = entries.map {
                                RecordingEntry(
                                    timestamp = it.timestamp,
                                    fileName = it.fileName,
                                    status = it.status,
                                    transcription = it.transcription,
                                    transcribedUsingModel = it.transcribedUsingModel,
                                    error = it.error,
                                    errorType = it.errorType,
                                    ringTransferInfo = it.ringTransferInfo,
                                    userMessageId = it.userMessageId,
                                )
                            },
                            messages = messages.map { it.document },
                        )
                        if (preferences.useEncryption.value) {
                            val encryptor: DocumentEncryptor = get()
                            val key = encryptor.getKey()
                            if (key != null) {
                                doc = encryptor.encryptDocument(doc, key)
                                logger.i { "Encrypted recording ${localRecording.id} before upload" }
                            } else {
                                logger.w { "Encryption enabled but no key available — uploading unencrypted" }
                            }
                        }
                        val existingFirestoreId = localRecording.firestoreId
                        val firestoreRecordingId = if (existingFirestoreId == null) {
                            val newId = firestoreRecordingsDao.newDocumentId()
                            recordingRepository.updateRecordingFirestoreId(localRecording.id, newId)
                            firestoreRecordingsDao.setRecording(newId, doc)
                            logger.i { "Uploaded recording ${localRecording.id} → $newId" }
                            newId
                        } else {
                            firestoreRecordingsDao.setRecording(existingFirestoreId, doc)
                            logger.i { "Pushed recording ${localRecording.id} → $existingFirestoreId" }
                            existingFirestoreId
                        }
                        recordingRepository.setLastPushedUpdated(
                            localRecording.id,
                            localRecording.updated.toEpochMilliseconds(),
                        )
                        val isFinal = entries.isNotEmpty() && entries.all {
                            it.status == RecordingEntryStatus.completed || it.status.isError()
                        }
                        if (isFinal) {
                            try {
                                val exporter: TraceSessionExporter = get()
                                val sessions = exporter.exportForRecording(localRecording.id)
                                if (sessions.isNotEmpty()) {
                                    val firestoreTracesDao: FirestoreTracesDao = get()
                                    firestoreTracesDao.setTrace(firestoreRecordingId, sessions)
                                    logger.i { "Uploaded trace (${sessions.size} sessions) for recording ${localRecording.id} → $firestoreRecordingId" }
                                }
                            } catch (e: Exception) {
                                logger.e(e) { "Error uploading trace for recording ${localRecording.id}" }
                            }
                        }
                    } catch (e: Exception) {
                        logger.e(e) { "Error uploading recording ${localRecording.id} to Firestore" }
                    } finally {
                        finishUpload(localRecording.id)
                    }
                }
            }
        }.flowOn(Dispatchers.IO).catch {
            logger.e(it) { "Error in local recording upload observer" }
        }.launchIn(scope)

        @OptIn(ExperimentalCoroutinesApi::class)
        authState().flatMapLatest { user ->
            val firestoreRecordingsDao: FirestoreRecordingsDao = get()
            if (user == null) flow<QuerySnapshot> {} else firestoreRecordingsDao.changesFlow()
        }.onEach { snap ->
            if (!preferences.backupEnabled.value) return@onEach
            var ingested = 0
            for (doc in snap.documents) {
                try {
                    val before = recordingRepository.getByFirestoreId(doc.id)
                    ingestRemoteRecording(doc.id, doc.data<RecordingDocument>())
                    val after = recordingRepository.getByFirestoreId(doc.id)
                    if (before == null || (after != null && after.updated != before.updated)) {
                        ingested++
                    }
                } catch (e: Exception) {
                    logger.w(e) { "auto-pull: skip ${doc.id}: ${e.message}" }
                }
            }
            var removed = 0
            for (change in snap.documentChanges) {
                if (change.type != dev.gitlive.firebase.firestore.ChangeType.REMOVED) continue
                val id = change.document.id
                val local = recordingRepository.getByFirestoreId(id) ?: continue
                try {
                    recordingRepository.deleteRecording(local.id)
                    removed++
                } catch (e: Exception) {
                    logger.w(e) { "auto-pull: failed to delete local ${local.id} for removed firestoreId $id: ${e.message}" }
                }
            }
            if (ingested > 0 || removed > 0) {
                logger.i { "auto-pull: ingested=$ingested removed=$removed" }
            }
        }.flowOn(Dispatchers.IO).catch {
            logger.e(it) { "Error in remote recording pull observer" }
        }.launchIn(scope)

        authState()
            .distinctUntilChanged { old, new -> old?.uid == new?.uid }
            .onEach { user ->
                if (user != null) {
                    try {
                        defaultListsBootstrap.ensure()
                    } catch (e: Exception) {
                        logger.w(e) { "DefaultListsBootstrap.ensure() failed" }
                    }
                }
            }
            .launchIn(scope)
    }

    private fun authState() = flow {
        emit(Firebase.auth.currentUser)
        Firebase.auth.authStateChanged.collect { emit(it) }
    }

    private suspend fun ingestRemoteRecording(firestoreId: String, document: RecordingDocument) {
        val recordingEntryDao: RecordingEntryDao = get()
        val conversationMessageDao: ConversationMessageDao = get()
        val encryptor: DocumentEncryptor = get()

        var recording = document
        if (recording.encrypted != null) {
            val key = encryptor.getKey()
            if (key != null) {
                recording = encryptor.decryptDocument(recording, key)
            } else {
                logger.w { "Encrypted recording $firestoreId but no key — storing encrypted" }
            }
        }

        val existing = recordingRepository.getByFirestoreId(firestoreId)
        if (existing != null && recording.updated <= existing.updated.toEpochMilliseconds()) return

        val localId = if (existing != null) {
            recordingRepository.updateRecording(
                existing.copy(
                    localTimestamp = recording.timestamp,
                    assistantTitle = recording.assistantSession?.title,
                    updated = Instant.fromEpochMilliseconds(recording.updated),
                ),
            )
            recordingEntryDao.deleteAllForRecording(existing.id)
            conversationMessageDao.deleteAllForRecording(existing.id)
            existing.id
        } else {
            recordingRepository.createRecording(
                firestoreId = firestoreId,
                localTimestamp = recording.timestamp,
                assistantTitle = recording.assistantSession?.title,
                updated = recording.updated,
            )
        }
        if (recording.entries.isNotEmpty()) {
            recordingEntryDao.insertRecordingEntries(
                recording.entries.map { entry ->
                    RecordingEntryEntity(
                        recordingId = localId,
                        timestamp = entry.timestamp,
                        fileName = entry.fileName,
                        status = entry.status,
                        transcription = entry.transcription,
                        transcribedUsingModel = entry.transcribedUsingModel,
                        error = entry.error,
                        errorType = entry.errorType,
                        ringTransferInfo = entry.ringTransferInfo,
                        userMessageId = entry.userMessageId,
                    )
                },
            )
        }
        recording.assistantSession?.messages?.takeIf { it.isNotEmpty() }?.let { messages ->
            conversationMessageDao.insertMessages(
                messages.map { ConversationMessageEntity(recordingId = localId, document = it) },
            )
        }
        recordingRepository.setRecordingUpdated(
            localId,
            Instant.fromEpochMilliseconds(recording.updated),
        )
        recordingRepository.setLastPushedUpdated(
            localId,
            Clock.System.now().toEpochMilliseconds(),
        )
    }
}
