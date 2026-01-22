package com.smartass.server.security;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class AuthKeyRegistry {
    private final Map<String, String> authKeys = new HashMap<String, String>();

    public AuthKeyRegistry() {
        authKeys.put("admin", "admin");
        authKeys.put("sensor-001", "key123");
        authKeys.put("light-001", "key456");
        authKeys.put("smoke-001", "key835");
        authKeys.put("energy-001", "key412");
        authKeys.put("motion-001", "key745");
        authKeys.put("fridge-001", "key997");
        authKeys.put("window-001", "key901");
    }

    public boolean isValid(String deviceId, String authKey) {
        return authKey != null && authKey.equals(authKeys.get(deviceId));
    }
}
