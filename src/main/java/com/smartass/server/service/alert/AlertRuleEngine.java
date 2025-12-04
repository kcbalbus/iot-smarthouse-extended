package com.smartass.server.service.alert;

import com.smartass.server.model.alert.AlertDTO;
import com.smartass.server.model.alert.AlertCondition;
import com.smartass.server.model.alert.ComparisonOperator;
import com.smartass.server.model.alert.AlertSeverity;
import com.smartass.server.model.alert.AlertType;
import com.smartass.server.model.device.DeviceData;
import com.smartass.server.registry.ConditionRegistry;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Component
public class AlertRuleEngine {

    private final ConditionRegistry conditionRegistry;

    public AlertRuleEngine(ConditionRegistry conditionRegistry) {
        this.conditionRegistry = conditionRegistry;
    }


    public List<AlertDTO> evaluateAll(DeviceData data) {
        String deviceType = data.getType();
        return conditionRegistry.getAllConditions().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(deviceType))
                .map(Map.Entry::getValue)
                .filter(condition -> evaluateSingleCondition(condition, data))
                .map(condition -> new AlertDTO(
                        data.getDeviceId(),
                        data.getType(),
                        condition.getSeverity(),
                        System.currentTimeMillis(),
                        condition.getDescription()
                ))
                .toList();
    }

    private boolean evaluateSingleCondition(AlertCondition condition, DeviceData data) {
        try {
            Field field = data.getClass().getDeclaredField(condition.getParameter());
            field.setAccessible(true);
            Object actualValue = field.get(data);
            return compareValues(actualValue, condition.getValue(), condition.getOperator());

        } catch (NoSuchFieldException | IllegalAccessException e) {
            System.err.println("Alert evaluation failed: " + e.getMessage());
            return false;
        }
    }

    private boolean compareValues(Object actual, String expectedValue, ComparisonOperator op) {
        if (actual instanceof Number) {
            double actualDouble = ((Number) actual).doubleValue();
            double expectedDouble;
            try {
                expectedDouble = Double.parseDouble(expectedValue);
            } catch (NumberFormatException e) {
                return false;
            }

            return switch (op) {
                case GREATER_THAN -> actualDouble > expectedDouble;
                case LESS_THAN -> actualDouble < expectedDouble;
                case EQUALS -> actualDouble == expectedDouble;
                case GREATER_OR_EQUAL -> actualDouble >= expectedDouble;
                case LESS_OR_EQUAL -> actualDouble <= expectedDouble;
                case NOT_EQUALS -> actualDouble != expectedDouble;
            };

        } else if (actual instanceof Boolean) {
            boolean actualBool = (Boolean) actual;
            boolean expectedBool = Boolean.parseBoolean(expectedValue);
            return switch (op) {
                case EQUALS -> actualBool == expectedBool;
                case NOT_EQUALS -> actualBool != expectedBool;
                default -> false;
            };

        } else if (actual instanceof String) {
            return switch (op) {
                case EQUALS -> actual.equals(expectedValue);
                case NOT_EQUALS -> !actual.equals(expectedValue);
                default -> false;
            };

        } else {
            return false;
        }
    }
}
