package com.example.flink.dto;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;



@JsonIgnoreProperties(ignoreUnknown = true)
public record TickRecord (
    @JsonProperty("open") double open,
    @JsonProperty("close") double close,
    @JsonProperty("high" ) double high,
    @JsonProperty("low") double low,
    @JsonProperty("volume") String volume,
    @JsonProperty("timestamp") long timestamp) {
}
