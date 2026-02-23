package com.jrp.research.repository;

import com.jrp.research.model.StockSignal;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface StockSignalRepository extends JpaRepository<StockSignal, Long> {
    StockSignal findByTicker(String ticker);
}
