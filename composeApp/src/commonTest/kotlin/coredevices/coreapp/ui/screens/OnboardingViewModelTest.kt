package coredevices.coreapp.ui.screens

import com.russhwolf.settings.MapSettings
import coredevices.util.CoreConfig
import coredevices.util.CoreConfigHolder
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class OnboardingViewModelTest {
    @Test
    fun phoneOnlyEnablesIndexDurablyWithoutHardwarePermissionsOrPebbleAuthentication() {
        val settings = MapSettings()
        val config = CoreConfigHolder(CoreConfig(enableIndex = false), settings, Json)
        val viewModel = OnboardingViewModel(config)

        viewModel.usePhoneOnly()

        assertTrue(config.config.value.enableIndex)
        assertEquals(OnboardingStage.SignIn, viewModel.stage.value)
        assertTrue(viewModel.requestedPermissions.value.isEmpty())
        assertTrue(CoreConfigHolder(CoreConfig(enableIndex = false), settings, Json).config.value.enableIndex)
    }
}
