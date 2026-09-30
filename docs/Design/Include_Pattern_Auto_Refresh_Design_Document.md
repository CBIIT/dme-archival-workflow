# Include Pattern Auto-Refresh Design Document

## 1. Problem Summary

Some DOC workflows require `DOC_SOURCE_RULE.INCLUDE_PATTERN` to be derived from date-based rules instead of being manually maintained in configuration. The implementation must support multiple pattern formats, including:

- **CSB**: rolling 3 short month names in lowercase, for example `jul/**,aug/**,sep/**`
- **MoCha BCL**: previous-month `yymm` dual-prefix format, for example `2608*/**,202608*/**`

The system must:

- resolve DOC config from the database-backed config service
- compute the expected include pattern from a configured strategy
- compare the expected value to the current DB-backed `INCLUDE_PATTERN`
- persist only when the value changes
- refresh the config cache immediately after a successful update

## 2. Shared Design Elements

### 2.1 Configuration Model

The latest `DOC_SOURCE_RULE` row for a DOC should carry:

- `INCLUDE_PATTERN`
- `INCLUDE_PATTERN_AUTO_UPDATE`
- `INCLUDE_PATTERN_AUTO_UPDATE_STRATEGY`

`INCLUDE_PATTERN_AUTO_UPDATE` determines whether the framework should derive the pattern automatically. `INCLUDE_PATTERN_AUTO_UPDATE_STRATEGY` selects the date-based generation rule.

### 2.2 Strategy Model

Pattern generation should be implemented through a small strategy abstraction, with one strategy per pattern format. Strategy names must be stable database values.

Initial strategies:

- `rolling-3-short-months-lowercase`
- `previous-month-yymm-dual-prefix`

### 2.3 Persistence Contract

The framework must never update `INCLUDE_PATTERN` blindly.

For every auto-refresh evaluation:

1. resolve current config
2. compute the expected include pattern
3. compare expected value with the DB-backed `INCLUDE_PATTERN`
4. update only if the value changed

### 2.4 Cache Contract

After a successful update, `DocConfigService` must refresh its cache immediately so the new include pattern is visible to the same runtime without waiting for the periodic config refresh.

## 3. Solution A: Conditional Persistence Without a Scheduled Task

### 3.1 Overview

The include-pattern refresh check is executed during normal DOC workflow execution. Before scanning the source path, the scheduler evaluates whether include-pattern auto-refresh is enabled for the DOC.

### 3.2 Execution Flow

1. Scheduler starts for a DOC.
2. Resolve the latest DOC config through `DocConfigService`.
3. Read `INCLUDE_PATTERN_AUTO_UPDATE`.
4. If the flag is disabled, continue normal workflow execution.
5. If the flag is enabled:
   - resolve the configured strategy
   - compute the expected include pattern for the current date
   - compare with the DB-backed `INCLUDE_PATTERN`
   - update only if the value changed
   - refresh config cache immediately after successful update
6. Continue workflow execution using the current config.

### 3.3 Advantages

- minimal moving parts
- no separate cron configuration
- no cross-server singleton scheduling requirement
- refresh occurs only for DOCs that actually run

### 3.4 Risks and Limitations

- if a DOC does not run for a long time, the pattern is not refreshed until the next execution
- the first run after a month boundary may perform an inline config update
- multiple servers can still reach the same conditional update path, although the compare-before-update logic reduces unnecessary writes

### 3.5 Best Fit

Use this solution when:

- pattern freshness at workflow execution time is sufficient
- operational simplicity is preferred over exact calendar-time refresh

## 4. Solution B: Conditional Persistence With a Scheduled Task

### 4.1 Overview

A framework-level scheduled job evaluates all DOCs with include-pattern auto-refresh enabled and updates the DB ahead of normal DOC execution.

### 4.2 Execution Flow

1. Scheduled job runs at the configured cadence.
2. Discover all eligible DOCs with auto-refresh enabled.
3. For each DOC:
   - resolve current config
   - resolve the configured strategy
   - compute the expected include pattern
   - compare with the DB-backed `INCLUDE_PATTERN`
   - update only if the value changed
   - refresh config cache immediately after successful update

### 4.3 Cross-Server Coordination

Because the application may run on multiple servers, only one server should execute the scheduled job for a given period.

Acceptable singleton controls include:

- dedicated database lock or lease table
- explicit scheduler-owner server configuration
- enterprise scheduler support for singleton execution

### 4.4 Advantages

- refresh can happen before any DOC execution begins
- refresh timing is predictable
- maintenance of auto-refresh patterns is centralized across all eligible DOCs

### 4.5 Risks and Limitations

- more infrastructure and coordination complexity
- requires careful handling of single-run behavior across servers
- adds additional operational surface area compared to execution-time refresh

### 4.6 Best Fit

Use this solution when:

- the business requirement is to refresh on a defined calendar schedule
- some DOCs may remain idle while config still needs to roll forward

## 5. Data Model

### 5.1 Required for Both Solutions

`DOC_SOURCE_RULE`

- `INCLUDE_PATTERN`
- `INCLUDE_PATTERN_AUTO_UPDATE`
- `INCLUDE_PATTERN_AUTO_UPDATE_STRATEGY`

### 5.2 Optional for Scheduled Solution

If scheduled singleton execution is required at the database layer, add a job lock or lease table dedicated to framework-level scheduled jobs.

## 6. Strategy Examples

### 6.1 CSB Strategy

Generate:

- `previous2/**,previous1/**,current/**`

Month names are short and lowercase.

### 6.2 MoCha BCL Strategy

Generate the previous-month `yymm` token in legacy dual-prefix format:

- `yymm*/**,20yymm*/**`

### 6.3 Strategy Design Rules

- strategy names must be durable DB values
- each strategy must be independently testable
- strategy logic must not embed DOC-specific scheduler branching

## 7. Persistence and Cache Behavior

Both solutions share the same persistence and cache rules:

1. resolve current config from the DB-backed config service
2. compute the expected include pattern
3. do nothing if the value is unchanged
4. update the latest `DOC_SOURCE_RULE` record if the value changed
5. refresh the config cache immediately after a successful update

This preserves alignment with the existing repository behavior for runtime config updates.

## 8. Operational Comparison

### 8.1 Without Scheduled Task

- simpler implementation
- lower operational maintenance
- refresh happens on demand

### 8.2 With Scheduled Task

- more predictable timing
- better for strict calendar-based refresh requirements
- higher coordination and operational complexity

## 9. Recommendation

Preferred default:

- **conditional persistence without a scheduled task**

Reasons:

- satisfies the compare-before-update requirement
- preserves immediate cache refresh behavior
- avoids cross-server singleton scheduling complexity

Use the scheduled-task design only when the requirement is explicitly that the database must be rolled forward on a fixed calendar schedule even when no DOC runs.

## 10. Decision Rule

Choose **no scheduled task** when:

- freshness at run time is acceptable
- simplicity is preferred

Choose **scheduled task** when:

- refresh must happen on a fixed date or time
- DOCs may stay idle while config still needs to advance
