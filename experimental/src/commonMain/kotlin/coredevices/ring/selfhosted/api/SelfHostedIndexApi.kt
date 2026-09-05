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
import io.ktor.client.request.prepareGet
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsChannel
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.json.json
import io.ktor.utils.io.readUTF8Line
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

class SelfHostedIndexAuthenticationException : Exception("Self-hosted Index authentication required")

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
    private val client = HttpClient(engine) {
        expectSuccess = true
        install(ContentNegotiation) {
            json(Json {
                encodeDefaults = true
                ignoreUnknownKeys = true
            })
        }
        HttpResponseValidator {
            handleResponseExceptionWithRequest { cause, _ ->
                val status = (cause as? ResponseException)?.response?.status
                if (status == HttpStatusCode.Unauthorized ||
                    status == HttpStatusCode.Forbidden
                ) {
                    throw SelfHostedIndexAuthenticationException()
                }
            }
        }
    }

    suspend fun enroll(password: String, deviceName: String): SelfHostedDevice {
        val enrollment = client.post(url("/v1/auth/enroll")) {
            contentType(ContentType.Application.Json)
            setBody(EnrollmentRequest(deviceName = deviceName, password = password))
        }.body<EnrollmentResponse>()

        enrollment.requireValid()
        tokenStorage.saveToken(TOKEN_KEY, enrollment.token)
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
        require(!response.hasMore || response.nextCursor < response.currentRevision)
        return response
    }

    suspend fun devices(): List<SelfHostedDevice> =
        client.get(url("/v1/devices")) {
            bearerAuth(requireToken())
        }.body<DeviceListResponse>().devices.onEach { it.requireValid() }

    suspend fun revokeDevice(deviceId: String) {
        revoke(deviceId)
    }

    suspend fun revokeCurrentDevice(deviceId: String) {
        revoke(deviceId)
        tokenStorage.deleteToken(TOKEN_KEY)
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

    private suspend fun revoke(deviceId: String) {
        require(deviceIdPattern.matches(deviceId))
        val response = client.delete(url("/v1/devices/$deviceId")) {
            bearerAuth(requireToken())
        }.body<RevocationResponse>()
        require(response.revoked)
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
