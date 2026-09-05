package coredevices.ring.selfhosted.capture

import coredevices.ring.data.entity.room.indexfeed.CachedList
import coredevices.ring.service.indexfeed.DefaultListsBootstrap

internal expect fun normalizeNfc(text: String): String

private const val WHITESPACE = "[\\s\\p{Z}\\uFEFF]"

/** Same prefix rules as the backend's noteFromTranscript, using synced lists. */
internal fun noteFromText(text: String, lists: List<CachedList>): Pair<String, String> {
    fun trimmed(value: String) = normalizeNfc(value).replace(Regex("^$WHITESPACE+|$WHITESPACE+$"), "")
    fun normalized(value: String) = trimmed(value).replace(Regex("$WHITESPACE+"), " ").lowercase()
    val normalizedText = normalized(text)
    val word = Regex("[\\p{L}\\p{N}_]")
    val list = lists.filter { list ->
        val name = normalized(list.title)
        val next = normalizedText.drop(name.length).take(1)
        !list.deleted && name.isNotEmpty() && normalizedText.startsWith(name) && (next.isEmpty() || !word.matches(next))
    }.sortedWith(compareByDescending<CachedList> { it.title.length }.thenBy { it.firestoreId }).firstOrNull()
        ?: return text to DefaultListsBootstrap.LIST_NOTES_SELF_ID
    val prefix = trimmed(list.title).split(Regex("$WHITESPACE+")).joinToString("$WHITESPACE+") { Regex.escape(it) }
    val nfc = normalizeNfc(text)
    val match = Regex("^$WHITESPACE*$prefix(?=$|[^\\p{L}\\p{N}_])", RegexOption.IGNORE_CASE).find(nfc)
    val title = if (match == null) text else nfc.drop(match.value.length).replace(Regex("^(?:$WHITESPACE|\\p{P})+"), "")
    return title to list.firestoreId
}
