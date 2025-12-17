package rating.engine.billingline.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import rating.engine.billingline.service.TestDataGenerator;
import rating.engine.billingline.service.ProcessService;

import static org.springframework.http.HttpStatus.ACCEPTED;

@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/process")
public class ProcessDataController {

    private final TestDataGenerator testDataGenerator;
    private final ProcessService processService;

    @PostMapping("/transform-data")
    @ResponseStatus(ACCEPTED)
    public void transformData() {
        log.info("Starting Bronze transformation");
        processService.transformAll();
    }

    @PostMapping("/test-data")
    @ResponseStatus(ACCEPTED)
    public void generateTestData() {
        log.info("Starting test data generation");
        testDataGenerator.generateTestData();
        log.info("Test data generation has been done");
    }

}