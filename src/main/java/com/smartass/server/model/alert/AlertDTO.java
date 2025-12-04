package com.smartass.server.model.alert;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class AlertDTO {
    private String deviceId;
    private String type;
    private AlertSeverity severity;
    private Long timestamp;
    private String description;


    public String getType() {
        return this.type;
    }
}
