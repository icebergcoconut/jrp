package com.jrp.research.model;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDate;

@Entity
@Table(name = "stock_backtests")
public class StockBacktest {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String ticker;
    private String companyName;
    private LocalDate decisionDate;
    private String decisionType; // BUY, SELL, HOLD
    private Double confidence;

    private BigDecimal closePrice;
    private Double rsi14;
    private Double fundamentalScore;
    
    public StockBacktest() {}

    public StockBacktest(String ticker, String companyName, LocalDate decisionDate, String decisionType, Double confidence, BigDecimal closePrice, Double rsi14, Double fundamentalScore) {
        this.ticker = ticker;
        this.companyName = companyName;
        this.decisionDate = decisionDate;
        this.decisionType = decisionType;
        this.confidence = confidence;
        this.closePrice = closePrice;
        this.rsi14 = rsi14;
        this.fundamentalScore = fundamentalScore;
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getTicker() { return ticker; }
    public void setTicker(String ticker) { this.ticker = ticker; }
    public String getCompanyName() { return companyName; }
    public void setCompanyName(String companyName) { this.companyName = companyName; }
    public LocalDate getBacktestDate() { return decisionDate; }
    public void setDecisionDate(LocalDate decisionDate) { this.decisionDate = decisionDate; }
    public String getBacktestType() { return decisionType; }
    public void setBacktestType(String decisionType) { this.decisionType = decisionType; }
    public Double getConfidence() { return confidence; }
    public void setConfidence(Double confidence) { this.confidence = confidence; }
    public BigDecimal getClosePrice() { return closePrice; }
    public void setClosePrice(BigDecimal closePrice) { this.closePrice = closePrice; }
    public Double getRsi14() { return rsi14; }
    public void setRsi14(Double rsi14) { this.rsi14 = rsi14; }
    public Double getFundamentalScore() { return fundamentalScore; }
    public void setFundamentalScore(Double fundamentalScore) { this.fundamentalScore = fundamentalScore; }
}
