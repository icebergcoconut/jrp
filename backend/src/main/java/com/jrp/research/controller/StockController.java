package com.jrp.research.controller;

import com.jrp.research.model.StockBacktest;
import com.jrp.research.repository.StockBacktestRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/stocks")
@CrossOrigin(origins = "http://localhost:5173") // Vite Default
public class StockController {

    private final StockBacktestRepository repository;

    public StockController(StockBacktestRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public ResponseEntity<List<StockBacktest>> getAllBacktests() {
        return ResponseEntity.ok(repository.findAll());
    }

    @GetMapping("/{ticker}")
    public ResponseEntity<StockBacktest> getBacktest(@PathVariable String ticker) {
        StockBacktest decision = repository.findByTicker(ticker);
        return decision != null ? ResponseEntity.ok(decision) : ResponseEntity.notFound().build();
    }
}
