package com.smartass.server.simulator.actuator;

import com.smartass.server.model.command.DeviceCommandDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.UUID;

@Profile("simulator")
public abstract class SimulatorActuatorBase {

    protected final Logger log = LoggerFactory.getLogger(getClass());
    private final KafkaReceiver<String, DeviceCommandDTO> commandReceiver;
    private final String deviceIdPrefix;
    private final String logPrefix;

    protected SimulatorActuatorBase(ReceiverOptions<String, DeviceCommandDTO> baseReceiverOptions,
                                    String groupIdPrefix,
                                    String deviceIdPrefix,
                                    String logPrefix) {
        this.deviceIdPrefix = deviceIdPrefix;
        this.logPrefix = logPrefix;

        ReceiverOptions<String, DeviceCommandDTO> options = baseReceiverOptions.consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, groupIdPrefix + UUID.randomUUID());
        this.commandReceiver = KafkaReceiver.create(options);
        log.info("{} starting actuator simulator with groupId={}", logPrefix, options.consumerProperties().get(ConsumerConfig.GROUP_ID_CONFIG));
        startListening();
    }

    private void startListening() {
        commandReceiver.receive()
                .doOnNext(this::handleRecord)
                .doOnError(err -> log.error("{} receiver error: {}", logPrefix, err.getMessage(), err))
                .subscribe();
    }

    private void handleRecord(ReceiverRecord<String, DeviceCommandDTO> record) {
        try {
            DeviceCommandDTO cmd = record.value();
            if (cmd == null) return;

            String deviceId = cmd.getDeviceId();
            if (deviceId == null || !deviceId.startsWith(deviceIdPrefix)) return;

            log.info("{} received command for device {}: command={}, value={}", logPrefix, deviceId, cmd.getCommand(), cmd.getValue());

            onCommand(cmd);

        } catch (Exception e) {
            log.error("{} error while handling command: {}", logPrefix, e.getMessage(), e);
        } finally {
            try { record.receiverOffset().acknowledge(); } catch (Exception ex) { log.warn("{} failed to ack offset: {}", logPrefix, ex.getMessage()); }
        }
    }

    // Hook for subclasses to extend behavior (no-op by default)
    protected void onCommand(DeviceCommandDTO cmd) {}
}

