package com.smartass.server.registry;

import com.smartass.server.model.device.*;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class DeviceDeserializerRegistry {
    private final Map<String, Class<? extends DeviceData>> registry = new HashMap<>();

    public DeviceDeserializerRegistry() {
        registry.put("temperature", TemperatureSensorData.class);
        registry.put("light", LightBulbData.class);
        registry.put("smoke", SmokeDetectorData.class);
        registry.put("energy", EnergyMeterData.class);
        registry.put("motion", MotionSensorData.class);
        registry.put("fridge", FridgeTemperatureSensorData.class);
        registry.put("window", WindowSensorData.class);
    }

    public Class<? extends DeviceData> resolve(String type) {
        return registry.get(type);
    }
}