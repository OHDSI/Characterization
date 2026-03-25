# Project Overview

This project is an R package that directly queries the OMOP Common Data
Model (CDM) to generate descriptive study results. It provides functions
for defining settings, running SQL-backed analyses against an OMOP CDM
instance, writing results to a database or files, and optionally
exploring results in a Shiny app.

Characterization is analysis-first: code should help users produce
reliable, transparent descriptive outputs from OMOP data with minimal
hidden behavior.

## Primary Goals

- Keep the codebase easy to understand, test, and extend for new
  contributors.
- Ensure OMOP CDM queries and resulting summaries are correct,
  reproducible, and explicit.
- Keep the Shiny experience intuitive for non-technical users and lay
  audiences when viewing produced results.
- Prefer maintainable, modular, and predictable code over clever one-off
  solutions.

## Folder Structure

- `/R`: Contains R functions for settings construction, analysis
  orchestration, database I/O, and app launch.
- `/inst`: Contains SQL used to query OMOP CDM tables, configuration
  files, and example data.
- `/man`: Contains documentation for the project, created using
  roxygen2.
- `/tests`: Contains testthat unit and integration tests.
- `/vignettes`: Contains R Markdown walkthroughs and usage guidance.

## OMOP CDM Query Principles

- Keep SQL logic deterministic and aligned with the intended OMOP CDM
  table semantics.
- Prefer explicit cohort, concept, and time-at-risk definitions over
  implicit defaults.
- Isolate SQL generation and execution from presentation/UI logic.
- Validate required OMOP inputs early with clear, actionable error
  messages.
- Preserve compatibility with supported database platforms when editing
  SQL templates.

## Contribution-Friendly Design Principles

- Favor small, single-responsibility functions and modules.
- Keep business/data logic separate from query execution and UI
  rendering logic.
- Prefer explicit inputs and outputs (clear function signatures, no
  hidden global dependencies).
- Reuse existing helper functions before adding new abstractions.
- Keep changes minimal and focused; avoid refactoring unrelated code in
  the same PR.
- If introducing a non-obvious pattern, document the rationale in
  roxygen or vignette notes.

## Analysis Pipeline Expectations

- Keep settings creation functions explicit, composable, and easy to
  reason about.
- Ensure orchestration functions call lower-level query helpers in a
  traceable order.
- Avoid hidden side effects in analysis functions; return or persist
  outputs consistently.
- Keep naming consistent across settings, SQL outputs, and exported
  result tables.

## User Experience Standards (Layperson-First)

- Write labels, titles, and help text in plain language; avoid jargon
  where possible.
- If technical terms are necessary, define them inline (short
  tooltip/help text).
- Present results in progressive detail: summary first, technical detail
  on demand.
- Prioritize readability of tables/plots (clear titles, units, legends,
  and sensible defaults).
- Use consistent naming for concepts across tabs, modules, and
  documentation.
- Handle empty/invalid/no-data states with clear guidance on what users
  should do next.
- Prefer predictable interactions over dense control panels.

## Documentation Expectations

- All exported functions must have complete roxygen2 docs with examples
  when feasible.
- For complex query modules, include a short “how it works” section in
  code comments or vignettes.
- Update `README.md` or vignettes when user-visible behavior changes.
- Keep terminology in docs aligned with UI text.
- Document key assumptions about OMOP CDM inputs, required tables, and
  expected output schema.

## Testing Expectations

- Add or update `testthat` tests for any behavior change in computation
  or data transformation.
- For query logic, test SQL generation paths and result-shaping helpers
  where feasible.
- For Shiny-related logic, test helper functions and core reactive logic
  where feasible.
- Cover edge cases: missing values, empty result sets, invalid inputs,
  and boundary conditions.
- Avoid brittle snapshot-like checks unless output stability is
  intentional.

## Libraries and Frameworks

- DBI/SqlRender/DatabaseConnector patterns in this codebase: For
  querying OMOP CDM-compatible databases.
- R shiny: For building the interactive result exploration application.
- roxygen2: For generating documentation from R code comments.
- testthat: For unit testing the package functions.

## Performance and Reliability

- Avoid repeated expensive OMOP queries or computations inside loops and
  reactive contexts.
- Cache or memoize only when it improves responsiveness and remains easy
  to reason about.
- Surface failures with actionable error messages for users and
  developers.
- Do not silently swallow errors that hide data quality or pipeline
  issues.

## Copilot Guidance for This Repository

- Match existing naming and file organization before proposing new
  patterns.
- Generate code that is explicit and readable for new contributors.
- Prefer incremental changes over large rewrites.
- Keep query-building and result-transformation logic auditable.
- When adding a new feature, suggest where tests and docs should be
  updated in the same change.
- Do not introduce new package dependencies unless clearly justified.

## Coding Standards

- We use camelCase in R. Function and variable names all start with
  lowercase. Package names start with uppercase.
- Function names typically start with a verb. Variable names are
  typically nouns. Do not encode the data type in the variable names.
  Also, everything is data, so no need to say that unless unavoidable.
- Place spaces around all infix operators (=, +, -, \<-, etc.). The same
  rule applies when using = in function calls. Always put a space after
  a comma, and never before (just like in regular English).
- Always indent the code inside curly braces. It’s ok to leave very
  short statements on the same line.
- Use \<-, not =, for assignment.
- When calling a function that has more than one argument, make sure to
  refer to each argument by name instead of relying on the order of
  arguments.

## Pull Request Checklist (for contributors and Copilot)

- Is the change understandable by a developer new to the project?
- Are function/module responsibilities clear and focused?
- Does the change preserve correctness of OMOP CDM query behavior?
- Are user-facing labels/messages plain language and consistent?
- Were tests and docs updated for behavior changes?
- Does the UI communicate key results simply before exposing advanced
  detail?
