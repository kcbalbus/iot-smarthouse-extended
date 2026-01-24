package com.smartass.server.simulator;

import com.smartass.server.model.command.DeviceCommandDTO;

public interface ActuatorUpdatable {
    /**
     * Apply a device command to the simulator. Return true if applied.
     */
    boolean applyCommand(DeviceCommandDTO command);
}
