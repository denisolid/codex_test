# Scanner V2 Runtime

## Live Integration Points

- `src/controllers/opportunitiesController.js`
  Feed, status, refresh, and insight requests now call `src/services/scannerV2Service.js` directly.
- `src/server.js`
  Scheduler startup and shutdown now start and stop only the `scanner_v2` scheduler path.
- `scripts/scanner-recovery.js`
  Recovery-triggered refreshes now enqueue `scanner_v2` directly.

## Active Source Of Truth

- Feed reads
  `src/services/scannerV2Service.js` reads from `global_active_opportunities` through `src/repositories/globalActiveOpportunityRepository.js`.
- Opportunity insight reads
  `src/services/opportunityInsightService.js` reads only active opportunity rows, so feed ids and insight ids stay on the same table path.
- Runtime status
  `src/services/scannerV2Service.js` reads scanner job state from `scanner_runs` and active feed count from `global_active_opportunities`.
- Publish and lifecycle writes
  `src/services/feed/globalFeedPublisher.js` writes to `global_active_opportunities`, `global_opportunity_history`, and `global_opportunity_lifecycle_log`.
- Revalidation writes
  `src/services/feed/feedRevalidationService.js` revalidates only the active v2 tables.

## Removed Legacy Pathing

- `src/services/scannerCompatibilityService.js`
  Removed. The app no longer keeps a runtime compatibility router between legacy and v2.
- `src/services/feed/feedCompatibilityProjector.js`
  Removed. `scanner_v2` no longer backfills `arbitrage_feed`.
- Legacy shadow and rollback flags
  Removed from the active scanner flow. There is no runtime dual-publisher or rollback switch in the cleaned-up path.

## Rollback

- Runtime rollback switch
  None. Post-cleanup rollback is an operational deploy revert to the previous release or commit.
- Expected rollback scope
  Restore the last build that still contains the legacy scanner wiring if emergency rollback is needed.
