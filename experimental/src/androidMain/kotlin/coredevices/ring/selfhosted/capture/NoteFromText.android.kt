package coredevices.ring.selfhosted.capture

import java.text.Normalizer

internal actual fun normalizeNfc(text: String): String = Normalizer.normalize(text, Normalizer.Form.NFC)
