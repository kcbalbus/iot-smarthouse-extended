package com.smartass.server.registry;

import com.smartass.server.model.alert.AlertCondition;
import com.smartass.server.model.alert.AlertSeverity;
import com.smartass.server.model.alert.ComparisonOperator;
import com.smartass.server.model.device.DeviceData;
import com.smartass.server.service.alert.AlertConditionValidator;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;

@Component
public class ConditionRegistry {
    /// In current version of the system there is only one permitted condition per parameter
    private final Map<String, AlertCondition> conditions = new ConcurrentHashMap<>();
    private final AlertConditionValidator alertConditionValidator;

    public ConditionRegistry(AlertConditionValidator alertConditionValidator) {
        this.alertConditionValidator = alertConditionValidator;

        //temperature
        conditions.put("temperature-high", new AlertCondition("temperature", "temperature",
                AlertSeverity.WARNING, ComparisonOperator.GREATER_THAN, "17",
                "Temperature is too high!"));

        conditions.put("temperature-temperature-critical", new AlertCondition(
                        "temperature", "temperature",
                        AlertSeverity.CRITICAL, ComparisonOperator.GREATER_THAN, "23",
                        "Critical temperature! Immediate action required."));

        conditions.put("temperature-temperature-low", new AlertCondition(
                "temperature", "temperature",
                AlertSeverity.WARNING, ComparisonOperator.LESS_THAN, "15",
                "Temperature is too low!"));


        conditions.put("temperature-temperature-freeze", new AlertCondition(
                "temperature", "temperature",
                AlertSeverity.CRITICAL, ComparisonOperator.LESS_THAN, "13",
                "Critical low temperature!"));



        conditions.put("temperature-humidity-low", new AlertCondition(
                "temperature", "humidity",
                AlertSeverity.WARNING, ComparisonOperator.LESS_THAN, "30",
                "Relative humidity is too low!"));

        conditions.put("temperature-humidity-high", new AlertCondition(
                "temperature", "humidity",
                AlertSeverity.WARNING, ComparisonOperator.GREATER_THAN, "70",
                "Relative humidity is too high!"));

        //light
        conditions.put("light-brightness-low", new AlertCondition(
                "light", "brightness",
                AlertSeverity.WARNING, ComparisonOperator.LESS_THAN, "5",
                "Brightness is very low!"));

        //energy
        conditions.put("energy-currentPower-high", new AlertCondition(
                "energy", "currentPower",
                AlertSeverity.WARNING, ComparisonOperator.GREATER_THAN, "150",
                "Current power consumption is too high!"));


        conditions.put("energy-currentPower-critical", new AlertCondition(
                "energy", "currentPower",
                AlertSeverity.CRITICAL, ComparisonOperator.GREATER_THAN, "300",
                "Critical power consumption!"));

        //motion
        conditions.put("motion-motionDetected", new AlertCondition(
                "motion", "motionDetected",
                AlertSeverity.WARNING, ComparisonOperator.EQUALS, "true",
                "Motion detected!"));

        //smoke
        conditions.put("smoke-alarmActive", new AlertCondition(
                "smoke", "alarmActive",
                AlertSeverity.CRITICAL, ComparisonOperator.EQUALS, "true",
                "Smoke/Fire detected!"));

        conditions.put("smoke-battery-low", new AlertCondition(
                "smoke", "batteryLevel",
                AlertSeverity.WARNING, ComparisonOperator.LESS_THAN, "20",
                "Smoke detector battery is low!"));

        //fridge
        conditions.put("fridge-temperature-high", new AlertCondition(
                "fridge", "temperature",
                AlertSeverity.WARNING, ComparisonOperator.GREATER_THAN, "8",
                "Temperature in the fridge is too high!"));

        conditions.put("fridge-temperature-critical", new AlertCondition(
                "fridge", "temperature",
                AlertSeverity.CRITICAL, ComparisonOperator.GREATER_THAN, "12",
                "Critical fridge temperature! Food may spoil."));

        conditions.put("fridge-temperature-low", new AlertCondition(
                "fridge", "temperature",
                AlertSeverity.WARNING, ComparisonOperator.LESS_THAN, "0",
                "Temperature in the fridge is too low!"));

        conditions.put("fridge-temperature-freeze", new AlertCondition(
                "fridge", "temperature",
                AlertSeverity.CRITICAL, ComparisonOperator.LESS_THAN, "-10",
                "Fridge temperature extremely low (freezing)!"));

        conditions.put("fridge-doorOpen", new AlertCondition(
                "fridge", "doorOpen",
                AlertSeverity.WARNING, ComparisonOperator.EQUALS, "true",
                "Fridge door is open!"));


    }

    public AlertCondition getCondition(String parameter) {
        return conditions.get(parameter);
    }

    public void setCondition(AlertCondition condition) {
        if (alertConditionValidator.validate(condition)) {
            String conditionID = condition.getDeviceType() + "-" + condition.getParameter();
            conditions.put(conditionID, condition);
        } else {
            throw new IllegalArgumentException("Invalid condition: " + condition);
        }
    }

    public void deleteCondition(String parameter) {
        conditions.remove(parameter);
    }

    public List<AlertCondition> getConditionsByDeviceType(String deviceType) {
        return conditions.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(deviceType))
                .map(Map.Entry::getValue)
                .toList();
    }

    public Map<String, AlertCondition> getAllConditions() {
        return Map.copyOf(conditions);
    }

}
