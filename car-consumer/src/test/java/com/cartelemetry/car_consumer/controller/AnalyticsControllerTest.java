package com.cartelemetry.car_consumer.controller;

import com.cartelemetry.car_consumer.dto.AnalyticsProcessResponse;
//import com.cartelemetry.car_consumer.service.AnalyticsService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AnalyticsControllerTest {
/**
    @Mock
    private AnalyticsService analyticsService;

    @InjectMocks
    private AnalyticsController analyticsController;

    @Test
    void triggerWhenNotRunning_returns202() {
        when(analyticsService.isRunning()).thenReturn(false);
        when(analyticsService.getLastTriggeredAt())
                .thenReturn(System.currentTimeMillis());

        ResponseEntity<AnalyticsProcessResponse> response =
                analyticsController.triggerProcessing();

        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        assertEquals("ACCEPTED", response.getBody().status());
        verify(analyticsService).processAnalytics();
    }

    @Test
    void triggerWhenAlreadyRunning_returns409() {
        when(analyticsService.isRunning()).thenReturn(true);
        when(analyticsService.getLastTriggeredAt())
                .thenReturn(System.currentTimeMillis());

        ResponseEntity<AnalyticsProcessResponse> response =
                analyticsController.triggerProcessing();

        assertEquals(HttpStatus.CONFLICT, response.getStatusCode());
        assertEquals("ALREADY RUNNING", response.getBody().status());
    }
    **/
}