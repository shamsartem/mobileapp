package coredevices.ring.selfhosted

import coredevices.indexai.data.entity.LocalRecording
import coredevices.indexai.data.entity.RecordingEntryEntity
import coredevices.indexai.data.entity.RecordingEntryStatus
import kotlin.test.Test
import kotlin.test.assertEquals

class VisibleRecordingsTest {
    private val recordings = (1L..4L).map { LocalRecording(id = it) }
    private val entries = listOf(
        RecordingEntryEntity(recordingId = 2, status = RecordingEntryStatus.pending),
        RecordingEntryEntity(recordingId = 3, status = RecordingEntryStatus.completed),
    )

    @Test
    fun hidesEmptyHistoryWithoutChangingSourceRows() {
        assertEquals(emptyList(), visibleRecordings(recordings, emptyList(), emptyList()))
        assertEquals(listOf(1L, 2L, 3L, 4L), recordings.map { it.id })
    }

    @Test
    fun selfHostedKeepsEntriesAndPreEntryTasksButHidesUnprocessedHistory() {
        assertEquals(listOf(2L, 3L, 4L),
            visibleRecordings(recordings, entries, listOf(4)).map { it.id })
        assertEquals(listOf(2L, 3L),
            visibleRecordings(recordings, entries, emptyList()).map { it.id })
        assertEquals(4, recordings.size)
    }
}
