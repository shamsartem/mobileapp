package coredevices.ring.storage

import coredevices.ring.selfhosted.api.SelfHostedIndexApi
import coredevices.ring.util.openReadChannel
import dev.gitlive.firebase.Firebase
import dev.gitlive.firebase.auth.auth
import dev.gitlive.firebase.storage.FirebaseStorageMetadata
import dev.gitlive.firebase.storage.storage
import io.ktor.utils.io.exhausted
import io.ktor.utils.io.readAvailable
import kotlinx.io.buffered
import kotlinx.io.files.SystemFileSystem
import kotlinx.io.readByteArray

data class RecordingBlob(
    val bytes: ByteArray,
    val sampleRate: Int,
    val encrypted: Boolean = false,
    val pcm: Boolean = false,
)

interface RecordingBlobStore {
    suspend fun get(id: String): RecordingBlob
    suspend fun put(id: String, bytes: ByteArray, metadata: Map<String, String>, recordingId: String?)
}

class SelfHostedRecordingBlobStore(private val api: SelfHostedIndexApi) : RecordingBlobStore {
    override suspend fun get(id: String): RecordingBlob {
        val audio = api.getAudio(id)
        return RecordingBlob(audio.bytes, audio.sampleRate)
    }

    override suspend fun put(id: String, bytes: ByteArray, metadata: Map<String, String>, recordingId: String?) {
        api.putAudio(requireNotNull(recordingId), id, bytes, metadata.getValue("sampleRate").toInt())
    }
}

class FirebaseRecordingBlobStore : RecordingBlobStore {
    override suspend fun get(id: String): RecordingBlob {
        val ref = Firebase.storage.reference("recordings/${Firebase.auth.currentUser!!.uid}/$id")
        val metadata = ref.getMetadata()
        val sampleRate = metadata?.customMetadata?.get("sampleRate")?.toInt()
            ?: error("Sample rate for recording $id not in firebase metadata")
        val temporary = recordingPath(getRecordingsCacheDirectory(), "$id.download.m4a")
        try {
            val channel = ref.openReadChannel()
            SystemFileSystem.sink(temporary).buffered().use { output ->
                val buffer = ByteArray(8192)
                while (!channel.exhausted()) {
                    val read = channel.readAvailable(buffer)
                    if (read > 0) output.write(buffer, 0, read)
                }
            }
            return RecordingBlob(
                SystemFileSystem.source(temporary).buffered().use { it.readByteArray() },
                sampleRate,
                encrypted = metadata.customMetadata?.get("encrypted") == "true",
                pcm = metadata.contentType == "audio/raw",
            )
        } finally {
            if (SystemFileSystem.exists(temporary)) SystemFileSystem.delete(temporary)
        }
    }

    override suspend fun put(id: String, bytes: ByteArray, metadata: Map<String, String>, recordingId: String?) {
        val temporary = recordingPath(getRecordingsCacheDirectory(), "$id.upload.m4a")
        SystemFileSystem.sink(temporary).buffered().use { it.write(bytes) }
        try {
            Firebase.storage.reference("recordings/${Firebase.auth.currentUser!!.uid}/$id").putFile(
                getFirebaseStorageFile(temporary),
                FirebaseStorageMetadata(contentType = "audio/mp4", customMetadata = metadata.toMutableMap()),
            )
        } finally {
            if (SystemFileSystem.exists(temporary)) SystemFileSystem.delete(temporary)
        }
    }
}
