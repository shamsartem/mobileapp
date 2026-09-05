package coredevices.ring.selfhosted

import androidx.room.migration.Migration
import androidx.sqlite.SQLiteConnection

/** Webhook schema 35 only added an unused delivery table to release schema 34. */
object PreserveWebhookDatabase : Migration(35, 34) {
    override fun migrate(connection: SQLiteConnection) {
        // Keep every table and row. Room validates the release schema before committing.
    }
}
