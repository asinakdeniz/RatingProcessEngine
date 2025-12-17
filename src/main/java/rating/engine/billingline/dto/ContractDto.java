package rating.engine.billingline.dto;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@Builder
public class ContractDto {

    private String contractNumber;

    private String customerId;

    private String meteringPointNumber;

    private Instant validFrom;

    private Instant validTo;

    private String status; // ACTIVE, TERMINATED, SUSPENDED

    private ProductDto product;

}