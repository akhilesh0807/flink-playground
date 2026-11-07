package com.example.flink.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MovingAvgAccumulator{
    public double sum = 0.0;
    public long count = 0L;
}
