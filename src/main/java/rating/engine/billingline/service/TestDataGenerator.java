package rating.engine.billingline.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import rating.engine.billingline.dto.ConsumptionDto;
import rating.engine.billingline.dto.ContractDto;
import rating.engine.billingline.dto.PriceDto;
import rating.engine.billingline.dto.ProductDto;
import rating.engine.billingline.persistence.ConsumptionRepository;
import rating.engine.billingline.persistence.ContractRepository;
import rating.engine.billingline.persistence.PriceRepository;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static java.time.Instant.now;

@Slf4j
@Service
@RequiredArgsConstructor
public class TestDataGenerator {

    private final ConsumptionRepository consumptionRepository;
    private final ContractRepository contractRepository;
    private final PriceRepository priceRepository;

    @Async
    public void generateTestData() {
        generatePrices();
        generateContracts();
        generateConsumption();
    }

    private void generatePrices() {
        List<PriceDto> prices = new ArrayList<>();
        Instant validFrom = now().minus(30, ChronoUnit.DAYS);
        Instant validTo = now().plus(365, ChronoUnit.DAYS);

        for (int i = 1; i <= 3; i++) {
            String productId = "PROD-" + i;

            prices.add(PriceDto.builder()
                    .priceId("PRICE-ENERGY-" + i)
                    .productId(productId)
                    .priceComponent("ENERGY")
                    .pricePerUnit(BigDecimal.valueOf(0.15))
                    .unit("kWh")
                    .validFrom(validFrom)
                    .validTo(validTo)
                    .countryCode("EE")
                    .build());

            prices.add(PriceDto.builder()
                    .priceId("PRICE-NETWORK-" + i)
                    .productId(productId)
                    .priceComponent("NETWORK")
                    .pricePerUnit(BigDecimal.valueOf(0.05))
                    .unit("kWh")
                    .validFrom(validFrom)
                    .validTo(validTo)
                    .countryCode("EE")
                    .build());
        }

        priceRepository.saveAll(prices);
        log.info("Generated {} prices", prices.size());
    }

    private void generateContracts() {
        int batchSize = 2000;
        int totalBatches = 4000;

        for (int i = 0; i < totalBatches; i++) {
            List<ContractDto> batch = new ArrayList<>();

            for (int j = 0; j < batchSize; j++) {
                long id = (long) i * batchSize + j;
                batch.add(createContractRecord(id));
            }

            contractRepository.saveAll(batch);
        }

        log.info("Generated {} contracts", totalBatches * batchSize);
    }

    private void generateConsumption() {
        int batchSize = 2000;
        int totalBatches = 4000;

        for (int i = 0; i < totalBatches; i++) {
            List<ConsumptionDto> batch = new ArrayList<>();

            for (int j = 0; j < batchSize; j++) {
                long id = (long) i * batchSize + j;
                batch.add(createConsumptionRecord(id));
            }

            consumptionRepository.saveAll(batch);
        }

        log.info("Generated {} consumption records", totalBatches * batchSize);
    }

    private ContractDto createContractRecord(Long id) {
        int productNum = (int) (id % 3) + 1;
        String productId = "PROD-" + productNum;

        ProductDto product = ProductDto.builder()
                .productId(productId)
                .productName("Standard Electricity Package " + productNum)
                .productType("ELECTRICITY")
                .countryCode("EE")
                .build();

        return ContractDto.builder()
                .contractNumber("CNT-" + id)
                .customerId("CUST-" + id)
                .meteringPointNumber("EE-MP-" + id)
                .validFrom(now().minus(30, ChronoUnit.DAYS))
                .validTo(now().plus(365, ChronoUnit.DAYS))
                .status("ACTIVE")
                .product(product)
                .build();
    }

    private ConsumptionDto createConsumptionRecord(Long id) {
        return ConsumptionDto.builder()
                .meteringPointNumber("EE-MP-" + id)
                .timestamp(now())
                .consumption(BigDecimal.valueOf(50 + (id % 50)))
                .unit("kWh")
                .customerId("CUST-" + id)
                .countryCode("EE")
                .qualityCode("VALID")
                .build();
    }
}