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
public class WindowActuatorSimulator {

    private final KafkaReceiver<String, DeviceCommandDTO> commandReceiver;

    public WindowActuatorSimulator(ReceiverOptions<String, DeviceCommandDTO> baseReceiverOptions) {
        ReceiverOptions<String, DeviceCommandDTO> options = baseReceiverOptions.consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "simulator-window-actuator-" + UUID.randomUUID());
        this.commandReceiver = KafkaReceiver.create(options);
        System.out.println("[ACTUATOR-WINDOW] starting actuator simulator with groupId=" + options.consumerProperties().get(ConsumerConfig.GROUP_ID_CONFIG));
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
            if (deviceId == null || !deviceId.startsWith("window-actuator-")) return;

            System.out.println("[ACTUATOR-WINDOW] received command for device " + deviceId + ": command=" + cmd.getCommand() + ", value=" + cmd.getValue());

        } catch (Exception e) {
            System.err.println("WindowActuatorSimulator error while handling command: " + e.getMessage());
        } finally {
            record.receiverOffset().acknowledge();
        }
    }
}
