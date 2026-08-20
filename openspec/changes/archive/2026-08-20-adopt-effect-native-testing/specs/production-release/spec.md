## ADDED Requirements

### Requirement: The complete test program is type- and lifecycle-checked
Release CI SHALL strictly typecheck production source, every test, every testing-support module, and public contract assertions before executing behavioral suites. Test completion SHALL also verify that suite-owned runtimes, Redis clients, containers, listeners, and fibers have been released.

#### Scenario: Test-only type error is introduced
- **WHEN** a release commit contains a strict TypeScript error only in an excluded test file
- **THEN** the release gate fails before publication

#### Scenario: Shared integration resource leaks
- **WHEN** an integration suite completes while a suite-owned resource remains undisposed
- **THEN** the release gate fails with lifecycle diagnostics
