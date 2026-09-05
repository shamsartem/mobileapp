package coredevices.ring.service.indexfeed

import coredevices.indexai.data.entity.ItemDocument
import coredevices.indexai.data.entity.RecordingEntry
import coredevices.indexai.data.entity.RecordingEntryEntity
import coredevices.indexai.data.entity.RecordingEntryStatus
import coredevices.ring.selfhosted.sync.IdentifiedDocument
import coredevices.ring.selfhosted.sync.MutationAcknowledgement
import coredevices.ring.selfhosted.sync.MutationKind
import coredevices.ring.selfhosted.sync.MutationOutcome
import coredevices.ring.selfhosted.sync.SyncDelta
import coredevices.ring.selfhosted.sync.SyncDocuments
import coredevices.ring.selfhosted.sync.SyncRequest
import coredevices.ring.selfhosted.sync.SyncResponse
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Instant
import kotlin.time.Duration.Companion.nanoseconds

class SelfHostedSyncMergeTest {
    private val empty = SyncDocuments(emptyList(), emptyList(), emptyList())
    private val item = IdentifiedDocument("item", ItemDocument(title = "local"))
    private val request = SyncRequest(1, 0, empty.copy(items = listOf(item)), emptyList())
    private val ack = MutationAcknowledgement(MutationKind.Item, "item", MutationOutcome.Accepted)
    private val response = SyncResponse(1, 1, 1, false, SyncDelta(empty, emptyList()), listOf(ack), SyncDelta(empty, emptyList()))

    @Test
    fun acknowledgementsMustCoverExactlyTheSubmittedKindsAndIds() {
        requireMatchingAcknowledgements(request, response)
        for (acks in listOf(emptyList(), listOf(ack, ack), listOf(ack.copy(id = "other")), listOf(ack.copy(kind = MutationKind.List)))) {
            assertFailsWith<IllegalArgumentException> {
                requireMatchingAcknowledgements(request, response.copy(acknowledgements = acks))
            }
        }
    }

    @Test
    fun rejectedMutationRequiresAuthoritativeStateEvenWithEmptyCursorPage() {
        val rejected = response.copy(acknowledgements = listOf(ack.copy(outcome = MutationOutcome.Conflict)))
        assertFailsWith<IllegalArgumentException> { requireMatchingAcknowledgements(request, rejected) }
        requireMatchingAcknowledgements(request, rejected.copy(
            reconciliation = SyncDelta(empty.copy(items = listOf(item.copy(document = item.document.copy(title = "winner")))), emptyList()),
        ))
    }

    @Test
    fun remoteCompletionKeepsEntryIdentityAndUnsentPendingEntries() {
        val first = RecordingEntryEntity(id = 7, recordingId = 2, timestamp = Instant.fromEpochMilliseconds(10), fileName = "a.wav", userMessageId = 3)
        val pending = first.copy(id = 8, timestamp = Instant.fromEpochMilliseconds(20), fileName = "b.wav")
        val obsolete = first.copy(id = 9, timestamp = Instant.fromEpochMilliseconds(30), status = RecordingEntryStatus.completed)
        val remote = first.toSyncDocument().copy(timestamp = first.timestamp + 123.nanoseconds, status = RecordingEntryStatus.completed, transcription = "note", userMessageId = 999)
        val merged = mergeRecordingEntries(2, listOf(first, pending, obsolete), listOf(remote))
        assertEquals(listOf(7L, 8L), merged.map { it.id })
        assertEquals("note", merged.first().transcription)
        assertEquals(3L, merged.first().userMessageId)
        assertEquals(pending, merged.last())
    }

    @Test
    fun remoteEntryCannotLinkToAnotherDevicesLocalMessageId() {
        val remote = RecordingEntry(timestamp = Instant.fromEpochMilliseconds(10), userMessageId = 999)
        val merged = mergeRecordingEntries(2, emptyList(), listOf(remote)).single()
        assertEquals(0L, merged.id)
        assertEquals(null, merged.userMessageId)
    }
}
