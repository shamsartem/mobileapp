package coredevices.ring.selfhosted.sync

import coredevices.indexai.data.entity.ItemDocument
import coredevices.indexai.data.entity.ListDocument
import coredevices.indexai.data.entity.RecordingDocument
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

const val SELF_HOSTED_SYNC_PROTOCOL_VERSION = 1

@Serializable
data class IdentifiedDocument<T>(
    val id: String,
    val document: T,
)

@Serializable
data class SyncDocuments(
    val recordings: List<IdentifiedDocument<RecordingDocument>>,
    val items: List<IdentifiedDocument<ItemDocument>>,
    val lists: List<IdentifiedDocument<ListDocument>>,
)

@Serializable
data class RecordingDeletionMutation(
    val id: String,
    val deletedAt: Long,
)

@Serializable
data class SyncRequest(
    val protocolVersion: Int,
    val cursor: Long,
    val mutations: SyncDocuments,
    val recordingDeletions: List<RecordingDeletionMutation>,
)

@Serializable
data class MutationAcknowledgement(
    val kind: MutationKind,
    val id: String,
    val outcome: MutationOutcome,
)

@Serializable
enum class MutationKind {
    @SerialName("recording")
    Recording,

    @SerialName("item")
    Item,

    @SerialName("list")
    List,

    @SerialName("recording_deletion")
    RecordingDeletion,
}

@Serializable
enum class MutationOutcome {
    @SerialName("accepted")
    Accepted,

    @SerialName("already_current")
    AlreadyCurrent,

    @SerialName("rejected_older")
    RejectedOlder,

    @SerialName("conflict")
    Conflict,
}

@Serializable
data class SyncDelta(
    val documents: SyncDocuments,
    val removedRecordingIds: List<String>,
)

@Serializable
data class SyncResponse(
    val protocolVersion: Int,
    val currentRevision: Long,
    val nextCursor: Long,
    val hasMore: Boolean,
    val changes: SyncDelta,
    val acknowledgements: List<MutationAcknowledgement>,
    val reconciliation: SyncDelta,
)

fun requireSupportedSyncProtocolVersion(protocolVersion: Int) {
    require(protocolVersion == SELF_HOSTED_SYNC_PROTOCOL_VERSION) {
        "Unsupported sync protocol version: $protocolVersion"
    }
}
