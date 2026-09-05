package coredevices.ring.selfhosted.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.unit.dp
import coredevices.ring.BuildKonfig
import coredevices.ring.database.Preferences
import coredevices.ring.selfhosted.api.SelfHostedIndexApi
import coredevices.ring.selfhosted.api.SelfHostedIndexAuthenticationException
import coredevices.ring.service.indexfeed.SelfHostedIndexSyncRuntime
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.launch
import org.koin.compose.koinInject

@Composable
fun SelfHostedIndexSettings() {
    val api = koinInject<SelfHostedIndexApi>()
    val runtime = koinInject<SelfHostedIndexSyncRuntime>()
    val preferences = koinInject<Preferences>()
    val authenticated by api.authenticated.collectAsState()
    val backupEnabled by preferences.backupEnabled.collectAsState()
    val scope = rememberCoroutineScope()
    var deviceName by remember { mutableStateOf("Android phone") }
    var password by remember { mutableStateOf("") }
    var busy by remember { mutableStateOf(false) }
    var message by remember { mutableStateOf("Checking connection…") }

    suspend fun perform(action: suspend () -> Unit) {
        busy = true
        message = "Connecting…"
        try {
            action()
        } catch (e: CancellationException) {
            throw e
        } catch (_: SelfHostedIndexAuthenticationException) {
            message = "Authorization was rejected. Sign in again."
        } catch (_: Exception) {
            message = "Could not connect. Check your connection and server, then try again."
        } finally {
            busy = false
        }
    }

    suspend fun sync() {
        api.devices()
        runtime.syncNow()
        message = "Connected. Sync complete."
    }

    LaunchedEffect(api) {
        perform {
            api.restoreAuthentication()
            if (api.authenticated.value) {
                api.devices()
                message = "Connected."
            } else {
                message = "Sign in to sync your notes."
            }
        }
    }

    Column(
        modifier = Modifier.fillMaxWidth().padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        Text("Self-hosted sync", style = MaterialTheme.typography.titleMedium)
        Text(BuildKonfig.SELF_HOSTED_BACKEND_URL, style = MaterialTheme.typography.bodySmall)
        Text(if (authenticated) "Signed in on this device" else "Signed out")
        Text(message, style = MaterialTheme.typography.bodySmall)
        if (!authenticated) {
            OutlinedTextField(
                value = deviceName,
                onValueChange = { deviceName = it },
                label = { Text("Device name") },
                singleLine = true,
                enabled = !busy,
                modifier = Modifier.fillMaxWidth(),
            )
            OutlinedTextField(
                value = password,
                onValueChange = { password = it },
                label = { Text("Server password") },
                singleLine = true,
                visualTransformation = PasswordVisualTransformation(),
                keyboardOptions = KeyboardOptions(keyboardType = KeyboardType.Password),
                enabled = !busy,
                modifier = Modifier.fillMaxWidth(),
            )
            Button(
                enabled = !busy && password.isNotEmpty() && deviceName.trim().length in 1..100,
                onClick = {
                    val enteredPassword = password
                    password = ""
                    scope.launch {
                        perform {
                            api.enroll(enteredPassword, deviceName.trim())
                            sync()
                        }
                    }
                },
            ) { Text("Sign in") }
        } else {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text("Automatic sync", modifier = Modifier.weight(1f))
                Switch(checked = backupEnabled, onCheckedChange = preferences::setBackupEnabled)
            }
            Row {
                Button(enabled = !busy, onClick = { scope.launch { perform { sync() } } }) {
                    Text("Sync now")
                }
                TextButton(enabled = !busy, onClick = {
                    scope.launch {
                        perform {
                            api.signOut()
                            message = "Signed out. Notes remain on this device."
                        }
                    }
                }) { Text("Sign out") }
            }
        }
    }
}
