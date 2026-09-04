@file:OptIn(ExperimentalTime::class)

package coredevices.ring.selfhosted.sync

import coredevices.indexai.data.entity.AssistantSessionDocument
import coredevices.indexai.data.entity.ItemDocument
import coredevices.indexai.data.entity.ListDocument
import coredevices.indexai.data.entity.RecordingDocument
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

class SelfHostedSyncProtocolTest {
    private val json = Json { encodeDefaults = true }
    private val timestamp = Instant.parse("2026-01-02T03:04:05Z")

    private val documents = SyncDocuments(
        recordings = listOf(
            IdentifiedDocument(
                id = "recording-id",
                document = RecordingDocument(
                    timestamp = timestamp,
                    updated = 1_767_322_245_000,
                    assistantSession = AssistantSessionDocument(title = "Recording"),
                ),
            ),
        ),
        items = listOf(
            IdentifiedDocument(
                id = "item-id",
                document = ItemDocument(
                    createdAt = timestamp,
                    updatedAt = timestamp,
                    title = "Buy coffee",
                    sourceRecordingId = "recording-id",
                ),
            ),
        ),
        lists = listOf(
            IdentifiedDocument(
                id = "list-id",
                document = ListDocument(
                    createdAt = timestamp,
                    updatedAt = timestamp,
                    title = "Shopping",
                ),
            ),
        ),
    )

    @Test
    fun requestRoundTripsEveryMutation() {
        val request = SyncRequest(
            protocolVersion = SELF_HOSTED_SYNC_PROTOCOL_VERSION,
            cursor = 41,
            mutations = documents,
            recordingDeletions = listOf(
                RecordingDeletionMutation(id = "deleted-recording-id", deletedAt = 1_767_322_245_000),
            ),
        )

        val encoded = json.encodeToString(request)

        assertEquals(request, json.decodeFromString(encoded))
        assertFalse(encoded.contains("audio"))
    }

    @Test
    fun nonAcceptedMutationsCarryAuthoritativeStateOutsideCursorChanges() {
        val nonAcceptedOutcomes = MutationOutcome.entries.filterNot { it == MutationOutcome.Accepted }
        val acknowledgements = MutationKind.entries.flatMap { kind ->
            nonAcceptedOutcomes.map { outcome ->
                MutationAcknowledgement(
                    kind = kind,
                    id = mutationId(kind, outcome),
                    outcome = outcome,
                )
            }
        }
        val reconciliation = SyncDelta(
            documents = SyncDocuments(
                recordings = acknowledgements.filter {
                    it.kind == MutationKind.Recording ||
                        it.kind == MutationKind.RecordingDeletion &&
                        it.outcome != MutationOutcome.AlreadyCurrent
                }.map { documents.recordings.single().copy(id = it.id) },
                items = acknowledgements.filter { it.kind == MutationKind.Item }
                    .map { documents.items.single().copy(id = it.id) },
                lists = acknowledgements.filter { it.kind == MutationKind.List }
                    .map { documents.lists.single().copy(id = it.id) },
            ),
            removedRecordingIds = acknowledgements.filter {
                it.kind == MutationKind.RecordingDeletion &&
                    it.outcome == MutationOutcome.AlreadyCurrent
            }.map { it.id },
        )
        val response = SyncResponse(
            protocolVersion = SELF_HOSTED_SYNC_PROTOCOL_VERSION,
            currentRevision = 50,
            nextCursor = 47,
            hasMore = true,
            changes = SyncDelta(
                documents = SyncDocuments(emptyList(), emptyList(), emptyList()),
                removedRecordingIds = emptyList(),
            ),
            acknowledgements = acknowledgements,
            reconciliation = reconciliation,
        )

        assertEquals(response, json.decodeFromString(json.encodeToString(response)))
        assertEquals(
            acknowledgements.map { it.id }.toSet(),
            buildSet {
                addAll(reconciliation.documents.recordings.map { it.id })
                addAll(reconciliation.documents.items.map { it.id })
                addAll(reconciliation.documents.lists.map { it.id })
                addAll(reconciliation.removedRecordingIds)
            },
        )
    }

    @Test
    fun documentIdsAndExistingDocumentFieldNamesStayOnTheWire() {
        val encoded = json.encodeToJsonElement(documents)
        val recordings = encoded.jsonObject.getValue("recordings").toString()
        val items = encoded.jsonObject.getValue("items").toString()

        assertTrue(recordings.contains("\"id\":\"recording-id\""))
        assertTrue(recordings.contains("\"assistant_session\""))
        assertTrue(items.contains("\"id\":\"item-id\""))
        assertTrue(items.contains("\"createdAt\""))
        assertTrue(items.contains("\"updatedAt\""))
        assertTrue(items.contains("\"sourceRecordingId\""))
        assertFalse(items.contains("created_at"))
    }

    @Test
    fun unsupportedProtocolVersionIsRejected() {
        requireSupportedSyncProtocolVersion(SELF_HOSTED_SYNC_PROTOCOL_VERSION)

        assertFailsWith<IllegalArgumentException> {
            requireSupportedSyncProtocolVersion(SELF_HOSTED_SYNC_PROTOCOL_VERSION + 1)
        }
    }

    private fun mutationId(kind: MutationKind, outcome: MutationOutcome): String =
        "${kind.name}-${outcome.name}"
}
