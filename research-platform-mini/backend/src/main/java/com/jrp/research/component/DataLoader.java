package com.jrp.research.component;

import com.jrp.research.model.StockSignal;
import com.jrp.research.repository.StockSignalRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

@Component
public class DataLoader implements CommandLineRunner {

    private final StockSignalRepository repository;

    public DataLoader(StockSignalRepository repository) {
        this.repository = repository;
    }

    @Override
    public void run(String... args) throws Exception {
        repository.deleteAll();

        repository.saveAll(List.of(
            new StockSignal("7203.T", "Toyota Motor", LocalDate.now(), "BUY", 0.85, new BigDecimal("3635"), 35.4, 82.5),
            new StockSignal("6758.T", "Sony Group", LocalDate.now(), "HOLD", 0.55, new BigDecimal("3336"), 50.1, 75.0),
            new StockSignal("9984.T", "SoftBank Group", LocalDate.now(), "SELL", 0.40, new BigDecimal("4329"), 85.2, 55.0),
            new StockSignal("6861.T", "Keyence", LocalDate.now(), "BUY", 0.90, new BigDecimal("61430"), 42.1, 88.0),
            new StockSignal("6098.T", "Recruit Holdings", LocalDate.now(), "HOLD", 0.60, new BigDecimal("6284"), 55.4, 70.0),
            new StockSignal("8306.T", "Mitsubishi UFJ FG", LocalDate.now(), "BUY", 0.82, new BigDecimal("2942"), 45.0, 78.5),
            new StockSignal("9432.T", "NTT", LocalDate.now(), "HOLD", 0.50, new BigDecimal("151"), 48.0, 60.5)
        ));

        System.out.println("✅ Data Loaded. Total signals: " + repository.count());
    }
}
