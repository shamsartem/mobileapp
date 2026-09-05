package coredevices.ring.storage

import kotlinx.io.files.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RecordingStoragePathTest {
    @Test
    fun recordingIdsCannotEscapeTheirDirectory() {
        val directory = Path("recordings")
        for (id in listOf("", ".", "..", "../outside", "folder/file", "folder\\file")) {
            assertFailsWith<IllegalArgumentException> { recordingPath(directory, id) }
        }
        assertEquals(Path(directory, "note.m4a"), recordingPath(directory, "note.m4a"))
    }
}
