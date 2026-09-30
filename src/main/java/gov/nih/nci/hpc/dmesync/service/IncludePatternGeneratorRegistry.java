package gov.nih.nci.hpc.dmesync.service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

@Component
public class IncludePatternGeneratorRegistry {

    private final Map<String, IncludePatternGenerator> generatorsByStrategy;

    public IncludePatternGeneratorRegistry(List<IncludePatternGenerator> generators) {
        this.generatorsByStrategy = generators.stream()
            .collect(Collectors.toMap(
                generator -> normalize(generator.getStrategyName()),
                Function.identity()));
    }

    public Optional<IncludePatternGenerator> find(String strategyName) {
        return Optional.ofNullable(generatorsByStrategy.get(normalize(strategyName)));
    }

    private String normalize(String strategyName) {
        return StringUtils.lowerCase(StringUtils.trimToEmpty(strategyName));
    }
}
