package rating.engine.billingline.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import rating.engine.billingline.dto.ContractDto;
import rating.engine.billingline.dto.PriceDto;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

@Repository
@RequiredArgsConstructor
public class PriceRepository {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public void saveAll(List<PriceDto> priceDtos) {
        if (priceDtos.isEmpty()) {
            return;
        }

        StringJoiner valuesJoiner = new StringJoiner(", ");
        List<Object> params = new ArrayList<>(priceDtos.size());

        for (PriceDto dto : priceDtos) {
            valuesJoiner.add("(?)");
            params.add(toJson(dto));
        }

        String sql = """
                INSERT INTO bronze.rating_engine.price (raw_json)
                VALUES 
                """ + valuesJoiner;

        jdbcTemplate.update(sql, params.toArray());
    }

    private String toJson(PriceDto dto) {
        try {
            return objectMapper.writeValueAsString(dto);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize to JSON", e);
        }
    }

}