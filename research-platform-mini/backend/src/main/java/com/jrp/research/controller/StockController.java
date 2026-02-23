package com.jrp.research.controller;

import com.jrp.research.model.StockSignal;
import com.jrp.research.repository.StockSignalRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/stocks")
@CrossOrigin(origins = "http://localhost:5173") // Vite Default
public class StockController {

    private final StockSignalRepository repository;

    public StockController(StockSignalRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public ResponseEntity<List<StockSignal>> getAllSignals() {
        return ResponseEntity.ok(repository.findAll());
    }

    @GetMapping("/{ticker}")
    public ResponseEntity<StockSignal> getSignal(@PathVariable String ticker) {
        StockSignal signal = repository.findByTicker(ticker);
        return signal != null ? ResponseEntity.ok(signal) : ResponseEntity.notFound().build();
    }
}
