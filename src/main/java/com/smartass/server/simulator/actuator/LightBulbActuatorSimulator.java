package com.smartass.server.simulator.actuator;

import com.smartass.server.model.command.DeviceCommandDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.UUID;

@Component
@Profile("simulator")
public class LightBulbActuatorSimulator {

    private final KafkaReceiver<String, DeviceCommandDTO> commandReceiver;

    public LightBulbActuatorSimulator(ReceiverOptions<String, DeviceCommandDTO> baseReceiverOptions) {
        ReceiverOptions<String, DeviceCommandDTO> options = baseReceiverOptions.consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "simulator-light-actuator-" + UUID.randomUUID());
        this.commandReceiver = KafkaReceiver.create(options);
        System.out.println("[ACTUATOR-LIGHT] starting actuator simulator with groupId=" + options.consumerProperties().get(ConsumerConfig.GROUP_ID_CONFIG));
        startListening();
    }

    private void startListening() {
        commandReceiver.receive()
                .doOnNext(this::handleRecord)
                .subscribe();
    }

    private void handleRecord(ReceiverRecord<String, DeviceCommandDTO> record) {
        try {
            DeviceCommandDTO cmd = record.value();
            if (cmd == null) return;

            String deviceId = cmd.getDeviceId();
            // Only log commands intended for light devices (prefix 'light-')
            if (deviceId == null || !deviceId.startsWith("light-")) return;

            System.out.println("[ACTUATOR-LIGHT] received command for device " + deviceId + ": command=" + cmd.getCommand() + ", value=" + cmd.getValue());

        } catch (Exception e) {
            System.err.println("LightBulbActuatorSimulator error while handling command: " + e.getMessage());
        } finally {
            record.receiverOffset().acknowledge();
        }
    }
}
