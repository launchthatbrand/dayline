package com.desmondtatilian.dayline.api;

import com.desmondtatilian.dayline.ledger.LedgerService;
import com.desmondtatilian.dayline.ledger.Views;
import com.desmondtatilian.dayline.stream.ItineraryStore;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class ConsoleController {

    private final LedgerService ledger;
    private final ItineraryStore itinerary;
    private final JdbcTemplate jdbc;

    public ConsoleController(LedgerService ledger, ItineraryStore itinerary, JdbcTemplate jdbc) {
        this.ledger = ledger;
        this.itinerary = itinerary;
        this.jdbc = jdbc;
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        jdbc.queryForObject("SELECT 1", Integer.class);
        return Map.of("status", "ok");
    }

    @GetMapping("/stats")
    public Views.StatsView stats() {
        return ledger.stats();
    }

    @GetMapping("/itinerary/{guestId}")
    public Views.ItineraryView itinerary(@PathVariable String guestId) {
        return new Views.ItineraryView(guestId, itinerary.list(guestId));
    }

    @PutMapping("/consumer")
    public Map<String, Boolean> consumer(@RequestBody FailRequest request) {
        ledger.setFailNext(request.failNext());
        return Map.of("failNext", request.failNext());
    }

    @PostMapping("/events/{eventId}/replay")
    public Map<String, String> replay(@PathVariable String eventId) {
        ledger.replay(eventId);
        return Map.of("eventId", eventId, "status", "queued");
    }

    public record FailRequest(boolean failNext) {
    }
}
