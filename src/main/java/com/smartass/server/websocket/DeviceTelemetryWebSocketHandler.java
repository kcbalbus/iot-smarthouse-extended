package com.smartass.server.websocket;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class DeviceTelemetryWebSocketHandler implements WebSocketHandler {

    private final List<WebSocketSession> sessions = new CopyOnWriteArrayList<>();

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        sessions.add(session);
        return session.receive().then()
                .doFinally(signal -> sessions.remove(session));
    }

    public void broadcast(String message) {
        Flux.fromIterable(sessions)
                .flatMap(session -> session.send(
                        Mono.just(session.textMessage(message))
                ))
                .subscribe();
    }

    public void broadcastTelemetry(String jsonPayload) {
        String wrapped = String.format("{\"type\":\"telemetry\",\"payload\":%s}", jsonPayload);
        Flux.fromIterable(sessions)
                .flatMap(session -> session.send(Mono.just(session.textMessage(wrapped))))
                .subscribe();
    }

    public void broadcastAlert(String jsonPayload) {
        String wrapped = String.format("{\"type\":\"alert\",\"payload\":%s}", jsonPayload);
        Flux.fromIterable(sessions)
                .flatMap(session -> session.send(Mono.just(session.textMessage(wrapped))))
                .subscribe();
    }

    public void broadcastScenario(String jsonPayload) {
        String wrapped = String.format("{\"type\":\"scenario\",\"payload\":%s}", jsonPayload);
        Flux.fromIterable(sessions)
                .flatMap(session -> session.send(Mono.just(session.textMessage(wrapped))))
                .subscribe();
    }

}
