package coredevices.ring.selfhosted.sync

import com.russhwolf.settings.MapSettings
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

class SelfHostedSyncStateTest {
    @Test
    fun cursorDefaultsToZero() {
        assertEquals(0, SelfHostedSyncState(MapSettings()).cursor)
    }

    @Test
    fun cursorPersistsAndCannotMoveBackwards() = runTest {
        val settings = MapSettings()
        val state = SelfHostedSyncState(settings)

        state.advanceCursor(42)
        state.advanceCursor(41)

        assertEquals(42, state.cursor)
        assertEquals(42, SelfHostedSyncState(settings).cursor)
    }

    @Test
    fun recordingDeletionAddIsIdempotentAndRemoveClearsIt() = runTest {
        val settings = MapSettings()
        val state = SelfHostedSyncState(settings)
        val deletion = RecordingDeletionMutation("recording-id", 1_767_322_245_000)

        state.addRecordingDeletion(deletion)
        state.addRecordingDeletion(deletion)

        assertEquals(listOf(deletion), state.pendingRecordingDeletions)

        state.removeRecordingDeletion(deletion.id)
        assertEquals(emptyList(), state.pendingRecordingDeletions)
        assertEquals(emptyList(), SelfHostedSyncState(settings).pendingRecordingDeletions)
    }

    @Test
    fun recordingDeletionsSurviveRestart() = runTest {
        val settings = MapSettings()
        val deletion = RecordingDeletionMutation("recording-id", 1_767_322_245_000)

        SelfHostedSyncState(settings).addRecordingDeletion(deletion)

        assertEquals(listOf(deletion), SelfHostedSyncState(settings).pendingRecordingDeletions)
    }

    @Test
    fun malformedStoredJsonFallsBackToNoRecordingDeletions() {
        val settings = MapSettings(SelfHostedSyncState.RECORDING_DELETIONS_KEY to "not json")

        assertEquals(emptyList(), SelfHostedSyncState(settings).pendingRecordingDeletions)
    }

    @Test
    fun invalidStoredRecordingDeletionsAreDiscarded() {
        val settings = MapSettings(
            SelfHostedSyncState.RECORDING_DELETIONS_KEY to
                """[
                    {"id":"", "deletedAt":1},
                    {"id":"recording-id", "deletedAt":2},
                    {"id":"negative", "deletedAt":-1},
                    {"id":"recording-id", "deletedAt":3}
                ]""".trimIndent()
        )

        assertEquals(
            listOf(RecordingDeletionMutation("recording-id", 2)),
            SelfHostedSyncState(settings).pendingRecordingDeletions,
        )
    }
}
