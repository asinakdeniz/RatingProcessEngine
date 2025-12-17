package rating.engine.billingline.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ProductDto {

    private String productId;

    private String productName;

    private String productType;

    private String countryCode;

}
