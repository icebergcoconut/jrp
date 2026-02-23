package com.jrp.research.repository;

import com.jrp.research.model.StockBacktest;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface StockBacktestRepository extends JpaRepository<StockBacktest, Long> {
    StockBacktest findByTicker(String ticker);
}
