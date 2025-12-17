package rating.engine.billingline.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
public class PriceDto {
    private String priceId;
    private String productId;
    private String priceComponent; // ENERGY, NETWORK, TAX, DISTRIBUTION
    private BigDecimal pricePerUnit;
    private String unit; // kWh, m3, etc.
    private Instant validFrom;
    private Instant validTo;
    private String countryCode;
}
