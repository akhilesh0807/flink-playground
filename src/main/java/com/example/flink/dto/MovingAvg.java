package com.example.flink.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record MovingAvg(
    @JsonProperty("timeframe") int timeframe,
    @JsonProperty("value") double value
) {
}
