package coredevices.ring.selfhosted

import coredevices.indexai.data.entity.LocalRecording
import coredevices.indexai.data.entity.RecordingEntryEntity

internal fun visibleRecordings(
    recordings: List<LocalRecording>,
    entries: List<RecordingEntryEntity>,
    pendingRecordingIds: List<Long>,
): List<LocalRecording> {
    // A staged capture can still be preprocessing before its first entry is committed.
    val visibleIds = entries.mapTo(mutableSetOf()) { it.recordingId } + pendingRecordingIds
    return recordings.filter { it.id in visibleIds }
}
