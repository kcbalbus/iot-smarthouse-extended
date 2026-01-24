package com.smartass.server.simulator.actuator;

import com.smartass.server.model.command.DeviceCommandDTO;
import com.smartass.server.simulator.SimulatorRegistry;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import reactor.kafka.receiver.ReceiverOptions;

@Component
@Profile("simulator")
public class MotionActuatorSimulator extends SimulatorActuatorBase {

    public MotionActuatorSimulator(ReceiverOptions<String, DeviceCommandDTO> baseReceiverOptions, SimulatorRegistry simulatorRegistry) {
        super(baseReceiverOptions, "simulator-motion-actuator-", "motion-actuator-", "[ACTUATOR-MOTION]", simulatorRegistry);
    }
}
