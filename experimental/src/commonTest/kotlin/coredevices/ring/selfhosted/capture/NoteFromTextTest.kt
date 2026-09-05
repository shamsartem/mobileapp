package coredevices.ring.selfhosted.capture

import coredevices.ring.data.entity.room.indexfeed.CachedList
import kotlin.test.Test
import kotlin.test.assertEquals

class NoteFromTextTest {
    @Test
    fun matchesBackendPrefixFixturesUsingSyncedLists() {
        val lists = listOf(
            CachedList("short", title = "Work"),
            CachedList("long", title = "Work ideas"),
            CachedList("unicode", title = "Café"),
        )
        for ((input, expected) in listOf(
            "  WORK\t IDEAS: Build it" to ("Build it" to "long"),
            "\uFEFFWORK\u00A0 IDEAS: Build it" to ("Build it" to "long"),
            "Cafe\u0301 — coffee" to ("coffee" to "unicode"),
            "Worker" to ("Worker" to "list_notes_self"),
            "Work_ideas" to ("Work_ideas" to "list_notes_self"),
        )) assertEquals(expected, noteFromText(input, lists))
        assertEquals("code" to "escaped", noteFromText("C++: code", listOf(CachedList("escaped", title = "C++"))))
        assertEquals("Projects: unchanged" to "list_notes_self", noteFromText("Projects: unchanged", emptyList()))
    }

    @Test
    fun ignoresDeletedListsAndUsesOrdinalIdForEqualTitles() {
        val lists = listOf(
            CachedList("a", title = "Work"),
            CachedList("B", title = "Work"),
            CachedList("long", title = "Work ideas", deleted = true),
        )
        assertEquals("ideas: next" to "B", noteFromText("Work ideas: next", lists))
    }
}
