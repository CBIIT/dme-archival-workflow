
package gov.nih.nci.hpc.dmesync.service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

/**
 * Date-based include pattern formats. The strategy names are stored in DOC_SOURCE_RULE and must not change.
 */
public enum IncludePatternStrategy {

  ROLLING_3_SHORT_MONTHS_LOWERCASE("rolling-3-short-months-lowercase",
      IncludePatternStrategy::rollingThreeShortMonths),

  PREVIOUS_MONTH_YYMM_DUAL_PREFIX("previous-month-yymm-dual-prefix",
      IncludePatternStrategy::previousMonthYyMmDualPrefix);

  private static final DateTimeFormatter YYMM_FORMATTER = DateTimeFormatter.ofPattern("yyMM");

  private final String strategyName;
  private final Function<LocalDate, String> generator;

  IncludePatternStrategy(String strategyName, Function<LocalDate, String> generator) {
    this.strategyName = strategyName;
    this.generator = generator;
  }

  public String getStrategyName() {
    return strategyName;
  }

  public String generate(LocalDate currentDate) {
    return generator.apply(currentDate);
  }

  public static Optional<IncludePatternStrategy> find(String strategyName) {
    String normalized = StringUtils.lowerCase(StringUtils.trimToEmpty(strategyName));
    return Arrays.stream(values())
        .filter(strategy -> strategy.strategyName.equals(normalized))
        .findFirst();
  }

  private static String rollingThreeShortMonths(LocalDate currentDate) {
    return Stream.of(currentDate.minusMonths(2), currentDate.minusMonths(1), currentDate)
        .map(date -> date.getMonth().getDisplayName(TextStyle.SHORT, Locale.ENGLISH).toLowerCase(Locale.ENGLISH) + "/**")
        .collect(Collectors.joining(","));
  }

  private static String previousMonthYyMmDualPrefix(LocalDate currentDate) {
    String monthToken = currentDate.minusDays(currentDate.getDayOfMonth()).format(YYMM_FORMATTER);
    return monthToken + "*/**," + "20" + monthToken + "*/**";
  }
}