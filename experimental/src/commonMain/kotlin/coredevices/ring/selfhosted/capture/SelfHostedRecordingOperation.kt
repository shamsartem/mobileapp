package coredevices.ring.selfhosted.capture

import androidx.room.Transactor
import androidx.room.useWriterConnection
import coredevices.indexai.data.entity.RecordingEntryEntity
import coredevices.indexai.data.entity.RecordingEntryErrorType
import coredevices.indexai.data.entity.RecordingEntryStatus
import coredevices.indexai.data.entity.RingTransferInfo
import coredevices.libindex.database.repository.RingTransferRepository
import coredevices.ring.database.Preferences
import coredevices.ring.database.room.RingDatabase
import coredevices.ring.database.room.repository.ItemRepository
import coredevices.ring.database.room.repository.ListRepository
import coredevices.ring.selfhosted.sync.SelfHostedSyncState
import coredevices.ring.service.indexfeed.ItemFactory
import coredevices.ring.service.indexfeed.SelfHostedIndexSyncRuntime
import coredevices.ring.service.recordings.RecordingProcessingQueue
import coredevices.ring.service.recordings.RecordingProcessingStage
import coredevices.ring.service.recordings.button.RecordingOperation
import coredevices.ring.storage.RecordingStorage
import coredevices.ring.storage.InvalidRecordingCaptureException
import coredevices.util.queue.RecoverableTaskException
import io.ktor.client.plugins.ClientRequestException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.first
import org.koin.core.component.KoinComponent
import org.koin.core.component.get
import kotlin.time.Clock
import kotlin.time.Instant

class SelfHostedAudioRecordingOperation(
    private val recordingId: Long,
    private val fileId: String,
    private val transferId: Long?,
) : RecordingOperation, KoinComponent {
    override suspend fun run(handle: RecordingProcessingQueue.TaskHandle?) {
        val db: RingDatabase = get()
        val transferInfo = transferId?.let { get<RingTransferRepository>().getRingTransferById(it)?.transferInfo }
        val entry = db.captureTransaction {
            if (db.localRecordingDao().getRecording(recordingId) == null) return@captureTransaction null
            db.ensureCaptureEntry(handle, recordingId, fileId, transferInfo = transferInfo)
        } ?: return
        transferId?.let { get<RingTransferRepository>().linkRecordingEntryToTransfer(it, entry.id) }
        if (entry.status == RecordingEntryStatus.completed || entry.status == RecordingEntryStatus.transcription_error) return
        if (!get<Preferences>().backupEnabled.value) throw RecoverableTaskException("Automatic backup is disabled")

        try {
            get<SelfHostedIndexSyncRuntime>().syncNow()
            val recording = db.localRecordingDao().getRecording(recordingId) ?: return
            val remoteId = requireNotNull(recording.firestoreId)
            if (get<SelfHostedSyncState>().pendingRecordingDeletions.any { it.id == remoteId }) return
            val status = db.recordingEntryDao().getById(entry.id)?.status ?: return
            if (status == RecordingEntryStatus.completed || status == RecordingEntryStatus.transcription_error) return
            get<RecordingStorage>().persistRecording(fileId, recordingId = remoteId)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            val invalid = e is InvalidRecordingCaptureException ||
                (e is ClientRequestException && e.response.status.value in listOf(400, 413))
            if (!invalid) throw RecoverableTaskException("Capture upload will retry", e)
            db.captureTransaction {
                db.recordingEntryDao().updateRecordingEntryStatus(
                    entry.id, RecordingEntryStatus.transcription_error,
                    error = "Recording could not be uploaded", errorType = RecordingEntryErrorType.transcription_failed,
                )
                db.touchCapture(recordingId)
            }
            throw e
        }
    }
}

class SelfHostedTextRecordingOperation(
    private val recordingId: Long,
    private val text: String,
) : RecordingOperation, KoinComponent {
    override suspend fun run(handle: RecordingProcessingQueue.TaskHandle?) {
        val db: RingDatabase = get()
        val items: ItemRepository = get()
        val lists: ListRepository = get()
        val factory: ItemFactory = get()
        db.captureTransaction {
            val recording = db.localRecordingDao().getRecording(recordingId) ?: return@captureTransaction
            val sourceId = requireNotNull(recording.firestoreId)
            db.ensureCaptureEntry(handle, recordingId, fileName = null, transcription = text)
            val itemId = "note_$sourceId"
            // A replay must not overwrite a note the user edited or deleted.
            if (items.getById(itemId) == null) {
                val note = noteFromText(text, lists.getAllFlow().first())
                items.upsertLocal(itemId, factory.noteItem(
                    sourceRecordingId = sourceId, createdAt = recording.localTimestamp,
                    title = note.first, resolvedListId = note.second, listHint = null, toolCallId = null,
                ))
            }
        }
    }
}

private suspend fun RingDatabase.ensureCaptureEntry(
    handle: RecordingProcessingQueue.TaskHandle?,
    recordingId: Long,
    fileName: String?,
    transcription: String? = null,
    transferInfo: RingTransferInfo? = null,
): RecordingEntryEntity {
    val dao = recordingEntryDao()
    val staged = (handle?.stage as? RecordingProcessingStage.RecordingEntryCreated)?.recordingEntryId
        ?.let { dao.getById(it) }
    val existing = staged ?: dao.getEntriesForRecording(recordingId).first().firstOrNull {
        it.fileName == fileName && (fileName != null || it.transcription == transcription)
    }
    val entryId = existing?.id ?: dao.insertRecordingEntry(RecordingEntryEntity(
        recordingId = recordingId, fileName = fileName, transcription = transcription,
        ringTransferInfo = transferInfo,
        status = if (transcription == null) RecordingEntryStatus.pending else RecordingEntryStatus.completed,
    ))
    handle?.updateStage(RecordingProcessingStage.RecordingEntryCreated(entryId, recordingId))
    if (existing == null) touchCapture(recordingId)
    return requireNotNull(dao.getById(entryId))
}

private suspend fun RingDatabase.touchCapture(recordingId: Long) {
    val local = localRecordingDao().getRecording(recordingId) ?: return
    localRecordingDao().setUpdated(recordingId, Instant.fromEpochMilliseconds(
        maxOf(Clock.System.now().toEpochMilliseconds(), local.updated.toEpochMilliseconds() + 1),
    ))
}

private suspend fun <T> RingDatabase.captureTransaction(block: suspend () -> T): T = useWriterConnection {
    it.withTransaction(Transactor.SQLiteTransactionType.IMMEDIATE) { block() }
}
