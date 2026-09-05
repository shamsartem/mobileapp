package coredevices.ring.ui.viewmodel

import androidx.compose.ui.platform.ClipEntry
import androidx.compose.ui.platform.Clipboard
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.paging.Pager
import androidx.paging.PagingConfig
import androidx.paging.cachedIn
import androidx.paging.filter
import androidx.paging.insertSeparators
import androidx.paging.map
import co.touchlab.kermit.Logger
import coredevices.indexai.database.dao.RecordingFeedItem
import coredevices.ring.agent.ContextualActionPredictor
import coredevices.ring.agent.builtin_servlets.clock.setTimer
import coredevices.ring.agent.integrations.UIEmailIntegration
import coredevices.libindex.database.entity.RingTransfer
import coredevices.libindex.database.dao.RingTransferDao
import coredevices.libindex.database.repository.RingTransferRepository
import coredevices.libindex.di.LibIndexCoroutineScope
import coredevices.ring.service.recordings.RecordingProcessingQueue
import coredevices.ring.database.room.repository.RecordingRepository
import coredevices.ring.BuildKonfig
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.datetime.LocalDate
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toLocalDateTime
import kotlin.time.Clock
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Instant

class FeedViewModel(
    private val transferRepo: RingTransferRepository,
    private val clipboard: Clipboard,
    private val uiMailIntegration: UIEmailIntegration,
    private val contextualActionPredictor: ContextualActionPredictor,
    private val ringTransferDao: RingTransferDao,
    private val recordingProcessingQueue: RecordingProcessingQueue,
    private val appScope: LibIndexCoroutineScope,
    recordingRepo: RecordingRepository,
): ViewModel() {
    companion object {
        private val logger = Logger.withTag(FeedViewModel::class.simpleName!!)
        private const val FEED_ITEMS_QUERY_LIMIT = 30
    }

    sealed class FeedData(val type: String) {
        sealed class WithTimestamp(val timestamp: Instant, type: String): FeedData(type)
        data class Item(val data: RecordingFeedItem): WithTimestamp(data.localTimestamp, "item")
        data class TransferPlaceholder(val data: RingTransfer): WithTimestamp(data.createdAt, "transfer_placeholder")
        data class DateSeparator(val date: LocalDate): FeedData("date_separator")
    }

    private val tz = TimeZone.currentSystemDefault()

    val items = Pager(
        config = PagingConfig(pageSize = FEED_ITEMS_QUERY_LIMIT, enablePlaceholders = false),
        pagingSourceFactory = { transferRepo.getPaginatedTransfersWithFeedItem() }
    ).flow.let { pages ->
        if (BuildKonfig.SELF_HOSTED_BACKEND_URL.isBlank()) pages
        else pages.cachedIn(viewModelScope).combine(recordingRepo.getDisplayRecordings()) { data, recordings ->
            val visibleIds = recordings.mapTo(mutableSetOf()) { it.id }
            data.filter { it.feedItem == null || it.feedItem!!.id in visibleIds }
        }
    }.map { data ->
        data
            .map {
                if (it.feedItem != null) {
                    FeedData.Item(it.feedItem!!)
                } else if (it.ringTransfer != null) {
                    FeedData.TransferPlaceholder(it.ringTransfer!!)
                } else {
                    error("Both feedItem and ringTransfer are null")
                }
            }
            .insertSeparators { before, after ->
                val afterDate = after?.timestamp?.toLocalDateTime(tz)?.date
                    ?: return@insertSeparators null
                val beforeDate = before?.timestamp?.toLocalDateTime(tz)?.date
                    ?: return@insertSeparators null
                if (beforeDate != afterDate) {
                    FeedData.DateSeparator(beforeDate) // Reverse layout, so before is later
                } else {
                    null
                }
            }
    }.onEach {
        logger.d { "refreshed feed items page" }
    }.cachedIn(viewModelScope)

    fun copyFeedItemTextToClipboard(text: String?) {
        viewModelScope.launch {
            text?.let {
                clipboard.setClipEntry(makeTextClipEntry(text))
            }
        }
    }

    fun emailFeedItemText(text: String) {
        viewModelScope.launch {
            try {
                uiMailIntegration.createNote(text)
            } catch (e: Exception) {
                logger.e(e) { "Failed to email feed item text" }
            }
        }
    }

    fun addToCalendar(text: String, timestamp: Instant, allDay: Boolean) {
        viewModelScope.launch {
            try {
                addCalendarEvent(
                    title = text,
                    startTime = timestamp,
                    endTime = timestamp + 30.minutes,
                    allDay = allDay
                )
            } catch (e: Exception) {
                logger.e(e) { "Failed to add calendar event" }
            }
        }
    }

    fun setAsTimer(text: String, timestamp: Instant) {
        val now = Clock.System.now()
        if (timestamp <= now) {
            logger.w { "Not setting timer because timestamp is in the past: $timestamp" }
            return
        }
        viewModelScope.launch {
            setTimer(timestamp - now, text, false)
        }
    }

    suspend fun getContextualActions(item: RecordingFeedItem) = contextualActionPredictor.getActions(item.id)

    fun retryFeedItem(item: RecordingFeedItem) {
        val entry = item.entry ?: return
        // appScope: the retry enqueue must land even if the user navigates
        // away before it commits.
        appScope.launch {
            val transfers = withContext(Dispatchers.IO) { ringTransferDao.getByRecordingId(item.id) }
            val transfer = transfers.firstOrNull()
            if (transfer != null) {
                recordingProcessingQueue.retryRecording(
                    transferId = transfer.id,
                    buttonSequence = null,
                    recordingId = item.id,
                    recordingEntryId = entry.id,
                )
            } else {
                val fileId = entry.fileName ?: return@launch
                recordingProcessingQueue.retryLocalRecording(
                    fileId = fileId,
                    buttonSequence = null,
                    recordingId = item.id,
                    recordingEntryId = entry.id,
                )
            }
        }
    }
}

expect suspend fun makeTextClipEntry(text: String): ClipEntry
expect suspend fun addCalendarEvent(title: String, startTime: Instant, endTime: Instant, allDay: Boolean)
