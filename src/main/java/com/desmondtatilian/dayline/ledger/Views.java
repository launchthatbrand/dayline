package com.desmondtatilian.dayline.ledger;

import java.util.List;

public final class Views {

    private Views() {
    }

    public record WindowView(String id, String attraction, String startsAt, int capacity, int remaining) {
    }

    public record ReservationView(
            String id,
            String guestId,
            String guestName,
            String windowId,
            String attraction,
            String startsAt,
            String status,
            String source) {
    }

    public record TapView(
            String idempotencyKey,
            String guestId,
            String windowId,
            String tapId,
            String result,
            String reservationId) {
    }

    public record DeadView(String eventId, String guestId, String eventType, String error, String createdAt) {
    }

    public record ItineraryItem(
            String guestId,
            String sk,
            String eventId,
            String type,
            String status,
            String guestName,
            String attraction,
            String startsAt,
            String windowId,
            String reservationId) {
    }

    public record ItineraryView(String guestId, List<ItineraryItem> items) {
    }

    public record StatsView(
            List<WindowView> windows,
            List<ReservationView> reservations,
            List<TapView> taps,
            List<DeadView> deadLetters,
            int unpublished,
            boolean failNext) {
    }
}
