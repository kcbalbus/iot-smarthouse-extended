package com.smartass.server.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartass.server.model.device.DeviceData;
import com.smartass.server.model.ErrorReason;
import com.smartass.server.security.AuthKeyRegistry;
import com.smartass.server.service.alert.AlertReactionService;
import com.smartass.server.service.processing.DeviceProcessingService;
import com.smartass.server.websocket.DeviceTelemetryWebSocketHandler;
import com.smartass.server.service.scenario.ScenarioService;
import com.smartass.server.model.alert.AlertDTO;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import java.util.List;

@Service
public class KafkaDeviceDataConsumerService {
    private static final Logger log = LoggerFactory.getLogger(KafkaDeviceDataConsumerService.class);

    private final KafkaReceiver<String, DeviceData> kafkaReceiver;
    private final AuthKeyRegistry authKeyRegistry;
    private final DeviceProcessingService processingService;
    private final MeterRegistry meterRegistry;
    private final DeviceTelemetryWebSocketHandler telemetryWebSocketHandler;
    private final ObjectMapper objectMapper;
    private final AlertReactionService alertDetectionService;
    private final ScenarioService scenarioService;

    public KafkaDeviceDataConsumerService(KafkaReceiver<String, DeviceData> kafkaReceiver,
                                          AuthKeyRegistry authKeyRegistry,
                                          DeviceProcessingService processingService,
                                          MeterRegistry meterRegistry,
                                          DeviceTelemetryWebSocketHandler telemetryWebSocketHandler,
                                          AlertReactionService alertReactionService,
                                          ScenarioService scenarioService) {
        this.kafkaReceiver = kafkaReceiver;
        this.authKeyRegistry = authKeyRegistry;
        this.processingService = processingService;
        this.meterRegistry = meterRegistry;
        this.telemetryWebSocketHandler = telemetryWebSocketHandler;
        this.objectMapper = new ObjectMapper();
        this.alertDetectionService = alertReactionService;
        this.scenarioService = scenarioService;

        startReceiving();
    }

    private void startReceiving() {
        System.out.println("startReceiving");
        kafkaReceiver.receive()
                .doOnNext(this::processRecord)
                .subscribe();
    }

    private void processRecord(ReceiverRecord<String, DeviceData> record) {
        try {
            DeviceData data = record.value();

            try {
                System.out.println("Telemetry received from device: " + (data != null ? data.getDeviceId() + " (type=" + data.getType() + ")" : "<null>"));
            } catch (Exception ex) {
            }

            if (!authKeyRegistry.isValid(data.getDeviceId(), data.getAuthKey())) {
                log.warn("[Kafka] Unauthorized device: {}", data.getDeviceId());
                meterRegistry.counter("iot.message.errors", "reason", ErrorReason.INVALID_AUTH.name()).increment();
                return;
            }

            processingService.handleGeneric(data);

            try {
                String json = objectMapper.writeValueAsString(data);
                telemetryWebSocketHandler.broadcastTelemetry(json);
            } catch (Exception ex) {
                log.warn("[WebSocket] Failed to send telemetry to frontend", ex);
            }

            List<AlertDTO> alerts = alertDetectionService.evaluateAndReact(data);
            alerts.forEach(alert -> {
                try {
                    String alertJson = objectMapper.writeValueAsString(alert);
                    telemetryWebSocketHandler.broadcastAlert(alertJson);
                } catch (Exception ex) {
                    log.warn("[WebSocket] Failed to send alert to frontend", ex);
                }
            });

            if (!alerts.isEmpty()) {
                scenarioService.executeForAlerts(alerts, data);
            }

            meterRegistry.counter("iot.message.success", "type", data.getType()).increment();

        } catch (Exception e) {
            log.error("[Kafka] Exception during message processing", e);
            meterRegistry.counter("iot.message.errors", "reason", ErrorReason.PROCESSING_ERROR.name()).increment();
        } finally {
            record.receiverOffset().acknowledge();
        }
    }

}
