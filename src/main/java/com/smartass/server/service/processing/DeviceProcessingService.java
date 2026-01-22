package com.smartass.server.service.processing;

import com.smartass.server.model.device.*;
import org.springframework.stereotype.Service;

@Service
public class DeviceProcessingService {

    public void handleGeneric(DeviceData data) {
        switch (data.getType()) {
            case "temperature" -> handleTemperature((TemperatureSensorData) data);
            case "light" -> handleLight((LightBulbData) data);
            case "smoke" -> handleSmoke((SmokeDetectorData) data);
            case "energy" -> handleEnergy((EnergyMeterData) data);
            case "motion" -> handleMotion((MotionSensorData) data);
            case "fridge" -> handleFridge((FridgeTemperatureSensorData) data);
            case "window" -> handleWindow((WindowSensorData) data);
            default -> System.out.println("Unknown device type");
        }
    }
    private void handleTemperature(TemperatureSensorData data) {
        System.out.println("[TEMPERATURE33] " + data.getTemperature() + "C, humidity: " + data.getHumidity());

    }
    private void handleLight(LightBulbData data) {
        System.out.println("[LIGHT] Status: " + (data.getState() ? "ON" : "OFF") + ", Brightness: " + data.getBrightness());

    }
    private void handleSmoke(SmokeDetectorData data) {
        System.out.println("[SMOKE] Alarm: " + (data.getAlarmActive() ? "ON" : "OFF")+", Smoke Level: "+ data.getSmokeLevel() + ", Battery Level: " + data.getBatteryLevel());

    }

    private void handleEnergy(EnergyMeterData data) {
        System.out.println("[Energy] Current Power: " + data.getCurrentPower()+", Total Energy: "+ data.getTotalEnergy());

    }

    private void handleMotion(MotionSensorData data) {
        System.out.println("[Motion] Any Motion: " + (data.getMotionDetected() ? "Yes" : "No"));

    }

    private void handleFridge(FridgeTemperatureSensorData data) {
        System.out.println("[Fridge] Door: " + (data.getDoorOpen() ? "Open" : "Close") + ", temperature: " + data.getTemperature());

    }

    private void handleWindow(WindowSensorData data) {
        System.out.println("[Window] State: " + (data.getIsOpen() != null && data.getIsOpen() ? "Open" : "Closed"));
    }
}
