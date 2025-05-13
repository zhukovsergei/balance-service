package ch.fdsgn.balance_service.controller;

import ch.fdsgn.balance_service.service.EventReplayService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EventReplayController {
    private final EventReplayService eventReplayService;
    
    public EventReplayController(EventReplayService eventReplayService) {
        this.eventReplayService = eventReplayService;
    }
    
    @PostMapping("/api/events/replay")
    public ResponseEntity<String> replayEvents() {
        try {
            eventReplayService.replayEvents();
            return ResponseEntity.ok("Event replay completed successfully");
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Failed: " + e.getMessage());
        }
    }
} 