package com.desmondtatilian.dayline.api;

import com.desmondtatilian.dayline.ledger.LedgerService;
import com.desmondtatilian.dayline.ledger.Views;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class CommandController {

    private final LedgerService ledger;

    public CommandController(LedgerService ledger) {
        this.ledger = ledger;
    }

    @PostMapping("/windows")
    public Views.WindowView createWindow(@RequestBody WindowRequest request) {
        return ledger.createWindow(request.attraction(), request.startsAt(), request.capacity());
    }

    @PostMapping("/reservations")
    public Views.ReservationView book(@RequestBody BookRequest request) {
        return ledger.book(request.guestName(), request.windowId(), "json");
    }

    @PostMapping(value = "/feeds/hotel", consumes = {MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE})
    public Views.ReservationView hotel(@RequestBody HotelStay stay) {
        int capacity = stay.capacity() == null || stay.capacity() < 1 ? 4 : stay.capacity();
        Views.WindowView window = ledger.createWindow(stay.attraction(), stay.windowStart(), capacity);
        return ledger.book(stay.guest(), window.id(), "xml");
    }

    @PostMapping("/reservations/{id}/cancel")
    public Views.ReservationView cancel(@PathVariable String id) {
        return ledger.cancel(id);
    }

    @PostMapping("/taps")
    public Views.TapView tap(@RequestBody TapRequest request) {
        return ledger.tap(request.guestId(), request.windowId(), request.tapId());
    }

    public record WindowRequest(String attraction, String startsAt, int capacity) {
    }

    public record BookRequest(String guestName, String windowId) {
    }

    public record TapRequest(String guestId, String windowId, String tapId) {
    }

    @JacksonXmlRootElement(localName = "hotelStay")
    public record HotelStay(
            @JacksonXmlProperty(isAttribute = true) String guest,
            @JacksonXmlProperty(isAttribute = true) String attraction,
            @JacksonXmlProperty(isAttribute = true) String windowStart,
            @JacksonXmlProperty(isAttribute = true) Integer capacity) {
    }
}
