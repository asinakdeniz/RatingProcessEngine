package rating.engine.billingline.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
public class ConsumptionDto {
    private String meteringPointNumber;
    private Instant timestamp;
    private BigDecimal consumption;
    private String unit;
    private String customerId;
    private String countryCode;
    private String qualityCode;
}