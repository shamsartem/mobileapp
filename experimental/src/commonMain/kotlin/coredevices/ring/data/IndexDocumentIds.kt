package coredevices.ring.data

object IndexDocumentIds {
    private const val ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

    fun newId(): String = buildString(20) {
        repeat(20) { append(ALPHABET.random()) }
    }
}
