package coredevices.ring.selfhosted.capture

import platform.Foundation.NSString
import platform.Foundation.create

internal actual fun normalizeNfc(text: String): String = NSString.create(string = text).precomposedStringWithCanonicalMapping
