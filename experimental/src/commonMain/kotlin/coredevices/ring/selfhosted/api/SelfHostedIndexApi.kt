package coredevices.ring.selfhosted.api

import coredevices.ring.selfhosted.sync.SyncRequest
import coredevices.ring.selfhosted.sync.SyncResponse
import coredevices.ring.selfhosted.sync.requireSupportedSyncProtocolVersion
import coredevices.util.integrations.IntegrationTokenStorage
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.HttpClientEngine
import io.ktor.client.plugins.HttpResponseValidator
import io.ktor.client.plugins.ResponseException
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.accept
import io.ktor.client.request.bearerAuth
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.put
import io.ktor.client.request.prepareGet
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsChannel
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.http.encodeURLPathPart
import io.ktor.serialization.kotlinx.json.json
import io.ktor.utils.io.readUTF8Line
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

class SelfHostedIndexAuthenticationException : Exception("Self-hosted Index authentication required")

data class SelfHostedAudio(val bytes: ByteArray, val sampleRate: Int)

@Serializable
data class SelfHostedDevice(
    val id: String,
    val name: String,
    val createdAtMs: Long,
)

class SelfHostedIndexApi(
    backendUrl: String,
    private val tokenStorage: IntegrationTokenStorage,
    engine: HttpClientEngine,
) {
    companion object {
        private const val TOKEN_KEY = "self_hosted_index_device_token"
        private val deviceIdPattern = Regex("[A-Za-z0-9_-]{22}")
        private val tokenPattern = Regex("[A-Za-z0-9_-]{43}")
    }

    private val baseUrl = backendUrl.trimEnd('/')
    private val authenticationMutex = Mutex()
    private var authenticationRestored = false
    private val _authenticated = MutableStateFlow(false)
    val authenticated = _authenticated.asStateFlow()

    private val client = HttpClient(engine) {
        expectSuccess = true
        install(ContentNegotiation) {
            json(Json {
                encodeDefaults = true
                ignoreUnknownKeys = true
            })
        }
        HttpResponseValidator {
            handleResponseExceptionWithRequest { cause, request ->
                val status = (cause as? ResponseException)?.response?.status
                if (status == HttpStatusCode.Unauthorized ||
                    status == HttpStatusCode.Forbidden
                ) {
                    val rejectedToken = request.headers[HttpHeaders.Authorization]?.removePrefix("Bearer ")
                    if (rejectedToken != null) clearAuthenticationForToken(rejectedToken)
                    throw SelfHostedIndexAuthenticationException()
                }
            }
        }
    }

    suspend fun restoreAuthentication() = authenticationMutex.withLock {
        if (!authenticationRestored) {
            _authenticated.value = tokenStorage.getToken(TOKEN_KEY) != null
            authenticationRestored = true
        }
    }

    suspend fun signOut() = authenticationMutex.withLock {
        clearAuthentication()
    }

    suspend fun enroll(password: String, deviceName: String): SelfHostedDevice {
        val enrollment = client.post(url("/v1/auth/enroll")) {
            contentType(ContentType.Application.Json)
            setBody(EnrollmentRequest(deviceName = deviceName, password = password))
        }.body<EnrollmentResponse>()

        enrollment.requireValid()
        authenticationMutex.withLock {
            tokenStorage.saveToken(TOKEN_KEY, enrollment.token)
            authenticationRestored = true
            _authenticated.value = true
        }
        return enrollment.device
    }

    suspend fun sync(request: SyncRequest): SyncResponse {
        requireSupportedSyncProtocolVersion(request.protocolVersion)
        require(request.cursor >= 0)
        val response = client.post(url("/v1/sync")) {
            bearerAuth(requireToken())
            contentType(ContentType.Application.Json)
            setBody(request)
        }.body<SyncResponse>()

        requireSupportedSyncProtocolVersion(response.protocolVersion)
        require(response.currentRevision >= 0)
        require(response.nextCursor in request.cursor..response.currentRevision)
        require(response.hasMore == (response.nextCursor < response.currentRevision))
        require(!response.hasMore || response.nextCursor > request.cursor)
        return response
    }

    suspend fun devices(): List<SelfHostedDevice> =
        client.get(url("/v1/devices")) {
            bearerAuth(requireToken())
        }.body<DeviceListResponse>().devices.onEach { it.requireValid() }

    suspend fun getAudio(blobId: String): SelfHostedAudio {
        val response = client.get(url("/v1/audio/${blobId.encodeURLPathPart()}")) {
            bearerAuth(requireToken())
        }
        require(response.contentType()?.match(ContentType.Audio.MP4) == true)
        val sampleRate = requireNotNull(response.headers["X-Audio-Sample-Rate"]?.toIntOrNull())
        require(sampleRate in 1..192_000)
        return SelfHostedAudio(response.body(), sampleRate)
    }

    suspend fun putAudio(recordingId: String, blobId: String, bytes: ByteArray, sampleRate: Int) {
        require(sampleRate in 1..192_000 && bytes.isNotEmpty() && bytes.size <= 16 * 1024 * 1024)
        val result = client.put(url(
            "/v1/recordings/${recordingId.encodeURLPathPart()}/audio/${blobId.encodeURLPathPart()}?sampleRate=$sampleRate"
        )) {
            bearerAuth(requireToken())
            contentType(ContentType.Audio.MP4)
            setBody(bytes)
        }.body<AudioUploadResponse>()
        require(result.blobId == blobId && Regex("[a-f0-9]{64}").matches(result.sha256))
    }

    suspend fun revokeDevice(deviceId: String) {
        revoke(deviceId)
    }

    suspend fun revokeCurrentDevice(deviceId: String) {
        clearAuthenticationForToken(revoke(deviceId))
    }

    private suspend fun clearAuthenticationForToken(token: String) = authenticationMutex.withLock {
        // A new enrollment may have replaced the token while this request was in flight.
        if (tokenStorage.getToken(TOKEN_KEY) == token) clearAuthentication()
    }

    private suspend fun clearAuthentication() {
        tokenStorage.deleteToken(TOKEN_KEY)
        authenticationRestored = true
        _authenticated.value = false
    }

    fun revisions(): Flow<Long> = flow {
        val token = requireToken()
        client.prepareGet(url("/v1/events")) {
            bearerAuth(token)
            accept(ContentType.Text.EventStream)
        }.execute { response ->
            require(response.contentType()?.match(ContentType.Text.EventStream) == true)
            val channel = response.bodyAsChannel()
            var event: String? = null
            var data: String? = null

            while (true) {
                val line = channel.readUTF8Line() ?: break
                when {
                    line.startsWith("event:") -> event = line.substringAfter(':').trimStart()
                    line.startsWith("data:") -> data = line.substringAfter(':').trimStart()
                    line.isEmpty() -> {
                        if (event == "revision") {
                            val revision = requireNotNull(data?.toLongOrNull())
                            require(revision >= 0)
                            emit(revision)
                        }
                        event = null
                        data = null
                    }
                }
            }
        }
    }

    fun close() {
        client.close()
    }

    private suspend fun revoke(deviceId: String): String {
        require(deviceIdPattern.matches(deviceId))
        val token = requireToken()
        val response = client.delete(url("/v1/devices/$deviceId")) {
            bearerAuth(token)
        }.body<RevocationResponse>()
        require(response.revoked)
        return token
    }

    private suspend fun requireToken(): String =
        tokenStorage.getToken(TOKEN_KEY) ?: throw SelfHostedIndexAuthenticationException()

    private fun url(path: String): String = "$baseUrl$path"

    private fun EnrollmentResponse.requireValid() {
        device.requireValid()
        require(tokenPattern.matches(token))
    }

    private fun SelfHostedDevice.requireValid() {
        require(deviceIdPattern.matches(id))
        require(name.isNotEmpty() && name == name.trim() && name.length <= 100)
        require(createdAtMs >= 0)
    }
}

@Serializable
private data class EnrollmentRequest(
    val deviceName: String,
    val password: String,
)

@Serializable
private data class EnrollmentResponse(
    val device: SelfHostedDevice,
    val token: String,
)

@Serializable
private data class DeviceListResponse(val devices: List<SelfHostedDevice>)

@Serializable
private data class RevocationResponse(val revoked: Boolean)

@Serializable
private data class AudioUploadResponse(val blobId: String, val sha256: String)
