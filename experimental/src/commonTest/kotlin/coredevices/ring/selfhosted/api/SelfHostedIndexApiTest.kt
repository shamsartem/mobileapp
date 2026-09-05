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
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
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
    fun audioUsesAuthenticatedBinaryTransportAndEncodedIdentifiers() = runTest {
        val bytes = byteArrayOf(0, 1, -1, 42)
        val api = SelfHostedIndexApi("https://index.example/base", TestTokenStorage(token), MockEngine { request ->
            assertEquals("Bearer $token", request.headers[HttpHeaders.Authorization])
            if (request.method == HttpMethod.Put) {
                assertEquals("/base/v1/recordings/record%20one/audio/blob%3Fone", request.url.encodedPath)
                assertEquals("16000", request.url.parameters["sampleRate"])
                assertEquals(ContentType.Audio.MP4, request.body.contentType)
                assertContentEquals(bytes, request.body.toByteArray())
                respondJson("""{"blobId":"blob?one","sha256":"${"a".repeat(64)}"}""")
            } else {
                assertEquals("/base/v1/audio/blob%3Fone", request.url.encodedPath)
                respond(bytes, headers = headersOf(
                    HttpHeaders.ContentType to listOf("audio/mp4"),
                    "X-Audio-Sample-Rate" to listOf("16000"),
                ))
            }
        })
        api.putAudio("record one", "blob?one", bytes, 16000)
        val downloaded = api.getAudio("blob?one")
        assertContentEquals(bytes, downloaded.bytes)
        assertEquals(16000, downloaded.sampleRate)
        api.close()
    }

    @Test
    fun audioRejectsInvalidMetadataAndMismatchedAcknowledgement() = runTest {
        for ((type, rate) in listOf("text/plain" to "16000", "audio/mp4" to "0", "audio/mp4" to "bad")) {
            val api = SelfHostedIndexApi("https://index.example", TestTokenStorage(token), MockEngine {
                respond(byteArrayOf(1), headers = headersOf(
                    HttpHeaders.ContentType to listOf(type),
                    "X-Audio-Sample-Rate" to listOf(rate),
                ))
            })
            assertFailsWith<IllegalArgumentException> { api.getAudio("blob") }
            api.close()
        }
        val api = SelfHostedIndexApi("https://index.example", TestTokenStorage(token), MockEngine {
            respondJson("""{"blobId":"different","sha256":"${"a".repeat(64)}"}""")
        })
        assertFailsWith<IllegalArgumentException> { api.putAudio("record", "blob", byteArrayOf(1), 16000) }
        api.close()
    }

    @Test
    fun audioRequiresAuthenticationAndValidUploadBeforeNetwork() = runTest {
        val api = SelfHostedIndexApi("https://index.example", TestTokenStorage(), MockEngine { error("No request expected") })
        assertFailsWith<SelfHostedIndexAuthenticationException> { api.getAudio("blob") }
        assertFailsWith<SelfHostedIndexAuthenticationException> { api.putAudio("record", "blob", byteArrayOf(1), 16000) }
        assertFailsWith<IllegalArgumentException> { api.putAudio("record", "blob", byteArrayOf(), 16000) }
        assertFailsWith<IllegalArgumentException> { api.putAudio("record", "blob", byteArrayOf(1), 0) }
        api.close()
    }

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
        assertTrue(api.authenticated.value)
        assertTrue(storage.savedKey?.isNotBlank() == true)
        api.close()
    }

    @Test
    fun storedAuthenticationSurvivesTransientFailureButRejectedTokenIsCleared() = runTest {
        val storage = TestTokenStorage(token)
        var requests = 0
        val api = SelfHostedIndexApi(
            "https://index.example",
            storage,
            MockEngine {
                respondError(if (requests++ == 0) HttpStatusCode.ServiceUnavailable else HttpStatusCode.Unauthorized)
            },
        )

        api.restoreAuthentication()
        assertTrue(api.authenticated.value)
        assertFailsWith<io.ktor.client.plugins.ServerResponseException> { api.devices() }
        assertEquals(token, storage.token)
        assertTrue(api.authenticated.value)

        assertFailsWith<SelfHostedIndexAuthenticationException> { api.devices() }
        assertNull(storage.token)
        assertFalse(api.authenticated.value)
        api.restoreAuthentication()
        assertFalse(api.authenticated.value)
        api.close()
    }

    @Test
    fun localSignOutClearsAuthenticationWithoutNetworkRequest() = runTest {
        val storage = TestTokenStorage(token)
        val api = SelfHostedIndexApi("https://index.example", storage, MockEngine { error("No network needed") })

        api.signOut()
        api.restoreAuthentication()

        assertNull(storage.token)
        assertFalse(api.authenticated.value)
        assertFailsWith<SelfHostedIndexAuthenticationException> { api.devices() }
        api.close()
    }

    @Test
    fun rejectedEnrollmentDoesNotClearExistingDeviceAuthentication() = runTest {
        val storage = TestTokenStorage(token)
        val api = SelfHostedIndexApi(
            "https://index.example",
            storage,
            MockEngine { respondError(HttpStatusCode.Forbidden) },
        )
        api.restoreAuthentication()

        assertFailsWith<SelfHostedIndexAuthenticationException> { api.enroll("wrong password", "Pixel") }

        assertEquals(token, storage.token)
        assertTrue(api.authenticated.value)
        api.close()
    }

    @Test
    fun lateRejectionOfOldTokenDoesNotClearNewEnrollment() = runTest {
        val storage = TestTokenStorage(token)
        val requestStarted = CompletableDeferred<Unit>()
        val rejectRequest = CompletableDeferred<Unit>()
        val newToken = "z".repeat(43)
        val api = SelfHostedIndexApi(
            "https://index.example",
            storage,
            MockEngine { request ->
                if (request.method == HttpMethod.Get) {
                    requestStarted.complete(Unit)
                    rejectRequest.await()
                    respondError(HttpStatusCode.Unauthorized)
                } else {
                    respondJson(
                        """{"device":{"id":"${device.id}","name":"Pixel","createdAtMs":0},"token":"$newToken"}""",
                    )
                }
            },
        )
        val pending = async {
            assertFailsWith<SelfHostedIndexAuthenticationException> { api.devices() }
        }
        requestStarted.await()
        api.enroll("secret", "Pixel")
        rejectRequest.complete(Unit)
        pending.await()

        assertEquals(newToken, storage.token)
        assertTrue(api.authenticated.value)
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

        for (status in listOf(HttpStatusCode.Unauthorized, HttpStatusCode.Forbidden)) {
            val rejectedTokenApi = SelfHostedIndexApi(
                "https://index.example",
                TestTokenStorage(token),
                MockEngine { respondError(status) },
            )
            assertFailsWith<SelfHostedIndexAuthenticationException> { rejectedTokenApi.devices() }
            rejectedTokenApi.close()
        }
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
