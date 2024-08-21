package io.axoniq.demo.bikerental.infra;

import com.google.type.DateTime;
import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.admin.AdminChannel;
import io.axoniq.axonserver.connector.command.CommandChannel;
import io.axoniq.axonserver.connector.control.ControlChannel;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.transformation.EventTransformationChannel;
import io.axoniq.axonserver.connector.query.QueryChannel;
import io.axoniq.demo.bikerental.rental.query.BikeStatusProjection;
import org.axonframework.axonserver.connector.AxonServerConfiguration;
import org.axonframework.axonserver.connector.AxonServerConnectionManager;
import org.axonframework.config.Configuration;
import org.axonframework.config.EventProcessingConfiguration;
import org.axonframework.eventhandling.*;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.messaging.StreamableMessageSource;
import org.axonframework.springboot.EventProcessorProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.springframework.web.server.ResponseStatusException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

@RestController()
@RequestMapping("/eventprocessors")
@Profile("axonserver")
public class EventProcessorController {

    private final EventProcessingConfiguration processingConfiguration;
    private final EventStore eventStore;
    private final EventProcessorProperties eventProcessorProperties;

    private final AxonServerConnection axonServerConnection;
    private final TokenStore tokenStore;

    static Logger logger = Logger.getLogger(EventProcessorController.class.getName());

    public EventProcessorController(EventProcessingConfiguration processingConfiguration,
                                    EventStore eventStore,
                                    EventProcessorProperties eventProcessorProperties,
                                    AxonServerConnectionManager axonServerConnectionManager,
                                    TokenStore tokenStore) {
        this.processingConfiguration = processingConfiguration;
        this.eventStore = eventStore;
        this.eventProcessorProperties = eventProcessorProperties;

        this.axonServerConnection = axonServerConnectionManager.getConnection();
        this.tokenStore = tokenStore;
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

    @PutMapping("/adminchannel/{eventProcessorName}/split")
    public void splitSegment(@PathVariable("eventProcessorName") String eventProcessorName) {
        AdminChannel adminChannel = this.axonServerConnection.adminChannel();
        adminChannel.splitEventProcessor(eventProcessorName, this.tokenStore.retrieveStorageIdentifier().get());
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

    @PutMapping("/adminchannel/{eventProcessorName}/merge")
    public void mergeSegment(@PathVariable("eventProcessorName") String eventProcessorName) {
        AdminChannel adminChannel = this.axonServerConnection.adminChannel();
        adminChannel.mergeEventProcessor(eventProcessorName, this.tokenStore.retrieveStorageIdentifier().get());
    }

    @PutMapping("/{eventProcessorName}/releasesegment/{segmentId}")
    public void releaseSegment(@PathVariable("eventProcessorName") String eventProcessorName, @PathVariable("segmentId") int segmentId  ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(streamingProcessor -> {
                    streamingProcessor.releaseSegment(segmentId, 10, TimeUnit.SECONDS);
                });
    }

    @PutMapping("/{eventProcessorName}/claimsegment/{segmentId}")
    public void claimSegment(@PathVariable("eventProcessorName") String eventProcessorName, @PathVariable("segmentId") int segmentId  ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(streamingProcessor -> {
                    streamingProcessor.claimSegment(segmentId);
                });
    }

    @PutMapping("/{eventProcessorName}/shutdown")
    public void shutdownProcessor(@PathVariable("eventProcessorName") String eventProcessorName ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(EventProcessor::shutDown);
    }

    @PutMapping("/adminchannel/{eventProcessorName}/pause")
    public void pauseProcessor(@PathVariable("eventProcessorName") String eventProcessorName) {
        AdminChannel adminChannel = this.axonServerConnection.adminChannel();
        adminChannel.pauseEventProcessor(eventProcessorName, this.tokenStore.retrieveStorageIdentifier().get());
    }


    @PutMapping("/{eventProcessorName}/start")
    public void startProcessor(@PathVariable("eventProcessorName") String eventProcessorName ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(EventProcessor::start);
    }

    @PutMapping("/adminchannel/{eventProcessorName}/start")
    public void adminChannelStartProcessor(@PathVariable("eventProcessorName") String eventProcessorName ) {
        AdminChannel adminChannel = this.axonServerConnection.adminChannel();
        adminChannel.startEventProcessor(eventProcessorName, this.tokenStore.retrieveStorageIdentifier().get());
    }


    /**
     *This method accepts the name of an event processor and attempts to reset the tokens for it to trigger a replay
     * from the date specified.
     * @param eventProcessorName - name of event processor trigger reset on
     * @param resetToDate - Date in format of YYYY-MM-DDTHH:MM:SS:mmmZ or the Global Token Id
     */
    @PutMapping("/{eventProcessorName}/initiatereplay/{resetToValue}")
    public void initiateReplayFromDate(@PathVariable("eventProcessorName") String eventProcessorName, @PathVariable("resetToValue") Optional<String> resetToValue ) {
        processingConfiguration.eventProcessor(eventProcessorName, StreamingEventProcessor.class)
                .ifPresent(streamingProcessor -> {
                    if (streamingProcessor.supportsReset()) {
                        try {
                            var result = this.axonServerConnection.adminChannel().pauseEventProcessor(eventProcessorName, tokenStore.retrieveStorageIdentifier().get());
                            result.join();
                            if(resetToValue.isPresent()) {
                                try {
                                    var replayFrom = Instant.parse(resetToValue.get());
                                    logger.info("Attempting to replay events from " + replayFrom.toString());
                                    streamingProcessor.resetTokens((messageSource) -> messageSource.createTokenAt(replayFrom));
                                } catch (DateTimeParseException pe) {
                                    //going to assume reset to a Global Token Id
                                    logger.info("Attempting to replay events from " + resetToValue.get().toString());
                                    var globalId = Long.parseLong(resetToValue.get());
                                    var token = new GlobalSequenceTrackingToken(globalId);
                                    streamingProcessor.resetTokens(token);
                                }
                            } else {
                                streamingProcessor.resetTokens();
                            }

                            result=this.axonServerConnection.adminChannel().startEventProcessor(eventProcessorName, tokenStore.retrieveStorageIdentifier().get());
                            result.join();

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