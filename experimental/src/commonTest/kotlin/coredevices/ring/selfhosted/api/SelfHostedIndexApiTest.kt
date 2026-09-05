package coredevices.ring.selfhosted.api

import coredevices.ring.selfhosted.sync.SELF_HOSTED_SYNC_PROTOCOL_VERSION
import coredevices.ring.selfhosted.sync.SyncDelta
import coredevices.ring.selfhosted.sync.SyncDocuments
import coredevices.ring.selfhosted.sync.SyncRequest
import coredevices.ring.selfhosted.sync.SyncResponse
import coredevices.util.integrations.IntegrationTokenStorage
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.respond
import io.ktor.client.engine.mock.respondError
import io.ktor.client.engine.mock.toByteArray
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SelfHostedIndexApiTest {
    private val json = Json { encodeDefaults = true }
    private val device = SelfHostedDevice(
        id = "abcdefghijklmnopqrstuv",
        name = "Pixel",
        createdAtMs = 1_700_000_000_000,
    )
    private val token = "abcdefghijklmnopqrstuvwxyzABCDEFGH_12345678"
    private val emptyDocuments = SyncDocuments(emptyList(), emptyList(), emptyList())

    @Test
    fun enrollmentUsesJoinedRouteAndStoresValidatedToken() = runTest {
        val storage = TestTokenStorage()
        val engine = MockEngine { request ->
            assertEquals("https://index.example/base/v1/auth/enroll", request.url.toString())
            assertEquals(HttpMethod.Post, request.method)
            assertEquals(
                "{\"deviceName\":\"Pixel\",\"password\":\"secret\"}",
                request.body.toByteArray().decodeToString(),
            )
            respondJson(
                """{"device":{"id":"${device.id}","name":"Pixel","createdAtMs":${device.createdAtMs}},"token":"$token"}""",
                HttpStatusCode.Created,
            )
        }
        val api = SelfHostedIndexApi("https://index.example/base/", storage, engine)

        assertEquals(device, api.enroll("secret", "Pixel"))
        assertEquals(token, storage.token)
        assertTrue(storage.savedKey?.isNotBlank() == true)
        api.close()
    }

    @Test
    fun invalidEnrollmentResponseDoesNotStoreToken() = runTest {
        val storage = TestTokenStorage()
        val api = SelfHostedIndexApi(
            "https://index.example",
            storage,
            MockEngine {
                respondJson(
                    """{"device":{"id":"${device.id}","name":"Pixel","createdAtMs":0},"token":"short"}""",
                    HttpStatusCode.Created,
                )
            },
        )

        assertFailsWith<IllegalArgumentException> { api.enroll("secret", "Pixel") }
        assertNull(storage.token)
        api.close()
    }

    @Test
    fun authenticatedRoutesUseBearerAndCurrentRevocationDeletesLocalToken() = runTest {
        val storage = TestTokenStorage(token)
        var requestNumber = 0
        val api = SelfHostedIndexApi(
            "https://index.example",
            storage,
            MockEngine { request ->
                assertEquals("Bearer $token", request.headers[HttpHeaders.Authorization])
                when (requestNumber++) {
                    0 -> {
                        assertEquals("https://index.example/v1/devices", request.url.toString())
                        assertEquals(HttpMethod.Get, request.method)
                        respondJson(
                            """{"devices":[{"id":"${device.id}","name":"Pixel","createdAtMs":${device.createdAtMs}}]}""",
                        )
                    }
                    else -> {
                        assertEquals("https://index.example/v1/devices/${device.id}", request.url.toString())
                        assertEquals(HttpMethod.Delete, request.method)
                        respondJson("""{"revoked":true}""")
                    }
                }
            },
        )

        assertEquals(listOf(device), api.devices())
        api.revokeCurrentDevice(device.id)
        assertNull(storage.token)
        assertTrue(storage.deletedKey?.isNotBlank() == true)
        api.close()
    }

    @Test
    fun revokingAnotherDeviceKeepsLocalToken() = runTest {
        val storage = TestTokenStorage(token)
        val api = SelfHostedIndexApi(
            "https://index.example",
            storage,
            MockEngine { respondJson("""{"revoked":true}""") },
        )

        api.revokeDevice(device.id)

        assertEquals(token, storage.token)
        api.close()
    }

    @Test
    fun missingAndRejectedTokensMapToAuthenticationException() = runTest {
        val missingTokenApi = SelfHostedIndexApi(
            "https://index.example",
            TestTokenStorage(),
            MockEngine { error("Request should not be sent") },
        )
        assertFailsWith<SelfHostedIndexAuthenticationException> { missingTokenApi.devices() }
        missingTokenApi.close()

        var rejection = 0
        val rejectedTokenApi = SelfHostedIndexApi(
            "https://index.example",
            TestTokenStorage(token),
            MockEngine {
                respondError(
                    if (rejection++ == 0) HttpStatusCode.Unauthorized else HttpStatusCode.Forbidden,
                )
            },
        )
        assertFailsWith<SelfHostedIndexAuthenticationException> { rejectedTokenApi.devices() }
        assertFailsWith<SelfHostedIndexAuthenticationException> { rejectedTokenApi.devices() }
        rejectedTokenApi.close()
    }

    @Test
    fun syncUsesProtocolDtosDirectlyAndRoundTrips() = runTest {
        val request = SyncRequest(
            protocolVersion = SELF_HOSTED_SYNC_PROTOCOL_VERSION,
            cursor = 4,
            mutations = emptyDocuments,
            recordingDeletions = emptyList(),
        )
        val response = syncResponse(nextCursor = 5, currentRevision = 5)
        val api = SelfHostedIndexApi(
            "https://index.example",
            TestTokenStorage(token),
            MockEngine { httpRequest ->
                assertEquals("https://index.example/v1/sync", httpRequest.url.toString())
                assertEquals("Bearer $token", httpRequest.headers[HttpHeaders.Authorization])
                assertEquals(request, json.decodeFromString(httpRequest.body.toByteArray().decodeToString()))
                respondJson(json.encodeToString(response))
            },
        )

        assertEquals(response, api.sync(request))
        api.close()
    }

    @Test
    fun unsupportedRequestAndResponseProtocolVersionsAreRejected() = runTest {
        var requests = 0
        val api = SelfHostedIndexApi(
            "https://index.example",
            TestTokenStorage(token),
            MockEngine {
                requests++
                respondJson(json.encodeToString(syncResponse(protocolVersion = 2)))
            },
        )
        val unsupportedRequest = SyncRequest(
            protocolVersion = 2,
            cursor = 0,
            mutations = emptyDocuments,
            recordingDeletions = emptyList(),
        )

        assertFailsWith<IllegalArgumentException> { api.sync(unsupportedRequest) }
        assertEquals(0, requests)
        assertFailsWith<IllegalArgumentException> {
            api.sync(unsupportedRequest.copy(protocolVersion = SELF_HOSTED_SYNC_PROTOCOL_VERSION))
        }
        assertEquals(1, requests)
        api.close()
    }

    @Test
    fun inconsistentSyncPaginationIsRejected() = runTest {
        val request = SyncRequest(
            protocolVersion = SELF_HOSTED_SYNC_PROTOCOL_VERSION,
            cursor = 4,
            mutations = emptyDocuments,
            recordingDeletions = emptyList(),
        )
        var responseNumber = 0
        val api = SelfHostedIndexApi(
            "https://index.example",
            TestTokenStorage(token),
            MockEngine {
                val response = when (responseNumber++) {
                    0 -> syncResponse(nextCursor = 5, currentRevision = 5).copy(hasMore = true)
                    1 -> syncResponse(nextCursor = 4, currentRevision = 5).copy(hasMore = true)
                    else -> syncResponse(nextCursor = 4, currentRevision = 5)
                }
                respondJson(
                    json.encodeToString(response),
                )
            },
        )

        assertFailsWith<IllegalArgumentException> { api.sync(request) }
        assertFailsWith<IllegalArgumentException> { api.sync(request) }
        assertFailsWith<IllegalArgumentException> { api.sync(request) }
        api.close()
    }

    @Test
    fun malformedRevisionEventIsRejected() = runTest {
        val api = SelfHostedIndexApi(
            "https://index.example",
            TestTokenStorage(token),
            MockEngine {
                respond(
                    content = "event: revision\ndata: invalid\n\n",
                    headers = headersOf(HttpHeaders.ContentType, ContentType.Text.EventStream.toString()),
                )
            },
        )

        assertFailsWith<IllegalArgumentException> { api.revisions().toList() }
        api.close()
    }

    @Test
    fun revisionsParsesOnlyAuthenticatedRevisionEvents() = runTest {
        val api = SelfHostedIndexApi(
            "https://index.example/",
            TestTokenStorage(token),
            MockEngine { request ->
                assertEquals("https://index.example/v1/events", request.url.toString())
                assertEquals("Bearer $token", request.headers[HttpHeaders.Authorization])
                assertEquals(ContentType.Text.EventStream.toString(), request.headers[HttpHeaders.Accept])
                respond(
                    content = ": heartbeat\n\nevent: ignored\ndata: text\n\n" +
                        "event: revision\nid: 7\ndata: 7\n\nevent: revision\ndata: 9\n\n",
                    headers = headersOf(HttpHeaders.ContentType, ContentType.Text.EventStream.toString()),
                )
            },
        )

        assertEquals(listOf(7L, 9L), api.revisions().toList())
        api.close()
    }

    private fun syncResponse(
        protocolVersion: Int = SELF_HOSTED_SYNC_PROTOCOL_VERSION,
        nextCursor: Long = 0,
        currentRevision: Long = nextCursor,
    ) = SyncResponse(
        protocolVersion = protocolVersion,
        currentRevision = currentRevision,
        nextCursor = nextCursor,
        hasMore = false,
        changes = SyncDelta(emptyDocuments, emptyList()),
        acknowledgements = emptyList(),
        reconciliation = SyncDelta(emptyDocuments, emptyList()),
    )

    private fun MockRequestHandleScope.respondJson(
        content: String,
        status: HttpStatusCode = HttpStatusCode.OK,
    ) = respond(
        content = content,
        status = status,
        headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()),
    )

    private class TestTokenStorage(initialToken: String? = null) : IntegrationTokenStorage {
        var token = initialToken
        var savedKey: String? = null
        var deletedKey: String? = null

        override suspend fun saveToken(key: String, token: String) {
            savedKey = key
            this.token = token
        }

        override suspend fun getToken(key: String): String? = token

        override suspend fun deleteToken(key: String) {
            deletedKey = key
            token = null
        }
    }
}
