package io.axoniq.demo.bikerental.infra;

import com.google.type.DateTime;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.demo.bikerental.rental.query.BikeStatusProjection;
import org.axonframework.config.Configuration;
import org.axonframework.config.EventProcessingConfiguration;
import org.axonframework.eventhandling.*;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.messaging.StreamableMessageSource;
import org.axonframework.springboot.EventProcessorProperties;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

@RestController()
@RequestMapping("/eventprocessors")
public class EventProcessorController {

    private final EventProcessingConfiguration processingConfiguration;
    private final EventStore eventStore;
    private final EventProcessorProperties eventProcessorProperties;

    static Logger logger = Logger.getLogger(EventProcessorController.class.getName());

    public EventProcessorController(EventProcessingConfiguration processingConfiguration,
                                    EventStore eventStore,
                                    EventProcessorProperties eventProcessorProperties) {
        this.processingConfiguration = processingConfiguration;
        this.eventStore = eventStore;
        this.eventProcessorProperties = eventProcessorProperties;
    }

    @GetMapping()
    public List<ProcessorStatusDTO> getAllProcessors() {

        var headToken = this.eventStore.createHeadToken();

        final Long headPosition = (headToken != null)? headToken.position().orElse(0):0L;

       return this.processingConfiguration.eventProcessors().keySet().stream().map(
               name -> {
                   var deadLetterQueue = this.processingConfiguration.deadLetterQueue(name);
                   var eventProc = this.processingConfiguration.eventProcessor(name, StreamingEventProcessor.class).get();
                   var activeThreads = (eventProc instanceof TrackingEventProcessor) ? eventProc.processingStatus().size()  : 1;
                   var availableThreads = (eventProc instanceof TrackingEventProcessor) ? eventProc.maxCapacity()  : 1;
                   var props = this.eventProcessorProperties.getProcessors().get(name);

                   var segments = eventProc.processingStatus().values().stream().map(
                           status -> {
                                var currentIndex = status.getCurrentPosition().orElse(0);
                                var trackingToken = status.getTrackingToken();
                                return new SegmentDTO(
                                        (long) status.getSegment().getSegmentId(),
                                        status.getSegment().getMask() + 1,
                                        (trackingToken !=null)? trackingToken.getClass().getSimpleName(): "NOT SET",
                                        currentIndex,
                                        status.isReplaying(),
                                        (headPosition - currentIndex),
                                        status.getSegment().mergeableSegmentId(),
                                        status.getSegment().splitSegmentId()
                            );
                           }).toList();

                   return  new ProcessorStatusDTO(
                           name,
                           eventProc.getTokenStoreIdentifier(),
                           eventProc.isRunning(),
                           eventProc.isError(),
                           eventProc.supportsReset(),
                           activeThreads,
                           availableThreads,
                           (props !=null)? props.getBatchSize() : 1,
                           eventProc.getClass().getSimpleName(),
                           deadLetterQueue.isPresent(),
                           segments
                   );
               }).toList();
    }

    @PutMapping("/{eventProcessorName}/splitsegment/{segmentId}")
    public void splitSegment(@PathVariable("eventProcessorName") String eventProcessorName,@PathVariable("segmentId") int segmentId ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(streamingProcessor -> {
                    // Use the result to check whether the operation succeeded
                    CompletableFuture<Boolean> result =
                            streamingProcessor.splitSegment(segmentId);
                });
    }

    @PutMapping("/{eventProcessorName}/mergesegment/{segmentId}")
    public void mergeSegment(@PathVariable("eventProcessorName") String eventProcessorName,@PathVariable("segmentId") int segmentId ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(streamingProcessor -> {
                    // Use the result to check whether the operation succeeded
                    CompletableFuture<Boolean> result =
                            streamingProcessor.mergeSegment(segmentId);
                });
    }

    @PutMapping("/{eventProcessorName}/releasesegment/{segmentId}")
    public void startProcessor(@PathVariable("eventProcessorName") String eventProcessorName, @PathVariable("segmentId") int segmentId  ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(streamingProcessor -> {
                    streamingProcessor.releaseSegment(segmentId, 10, TimeUnit.SECONDS);
                });
    }
    @PutMapping("/{eventProcessorName}/shutdown")
    public void shutdownProcessor(@PathVariable("eventProcessorName") String eventProcessorName ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(EventProcessor::shutDown);
    }

    @PutMapping("/{eventProcessorName}/start")
    public void startProcessor(@PathVariable("eventProcessorName") String eventProcessorName ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(EventProcessor::start);
    }

    /**
     *This method accepts the name of an event processor and attempts to reset the tokens for it to trigger a replay
     * from the date specified.
     * @param eventProcessorName - name of event processor trigger reset on
     * @param resetToDate - Date in format of YYYY-MM-DDTHH:MM:SS:mmm
     */
    @PutMapping("/{eventProcessorName}/initiatereplay/{resetToDate}")
    public void initiateReplayFromDate(@PathVariable("eventProcessorName") String eventProcessorName, @PathVariable("resetToDate") Optional<String> resetToDate ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(streamingProcessor -> {
                    if (streamingProcessor.supportsReset()) {
                        try {
                            if(resetToDate.isPresent()) {
                                var replayFrom = Instant.parse(resetToDate.get());
                                logger.info("Attempting to replay events from " + replayFrom.toString());
                                streamingProcessor.shutDown();
                                streamingProcessor.resetTokens((messageSource) -> messageSource.createTokenAt(replayFrom));
                            } else {
                                streamingProcessor.shutDown();
                                streamingProcessor.resetTokens();
                            }

                            streamingProcessor.start();
                        } catch (UnableToClaimTokenException ex) {
                            throw new ResponseStatusException(HttpStatus.CONFLICT, ex.getLocalizedMessage());
                        }
                    }
                });
    }

    /**
     * This will trigger a full reset from the beginning of the event stream for the named Event Processor
     *
     * @param eventProcessorName - name of event processor trigger reset on
     */
    @PutMapping("/{eventProcessorName}/initiatereplay")
    public void initiateReplayFromTail(@PathVariable("eventProcessorName") String eventProcessorName) {
        initiateReplayFromDate(eventProcessorName, Optional.empty());
    }

}