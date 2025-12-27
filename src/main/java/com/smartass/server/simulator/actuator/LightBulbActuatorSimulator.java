package com.smartass.server.simulator.actuator;

import com.smartass.server.model.command.DeviceCommandDTO;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.kafka.receiver.ReceiverOptions;

@Component
@Profile("simulator")
public class LightBulbActuatorSimulator extends SimulatorActuatorBase {

    public LightBulbActuatorSimulator(ReceiverOptions<String, DeviceCommandDTO> baseReceiverOptions) {
        super(baseReceiverOptions, "simulator-light-actuator-", "light-actuator-", "[ACTUATOR-LIGHT]");
    }
}
