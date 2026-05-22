# Evaluation of Permissions

**Status:** Draft design
**Owner:** Palimpsest

## Summary

Palimpsest already has a permission-rule DSL that rewrites subscriptions so
clients only receive rows they are allowed to see. This feature adds a formal
permission evaluation system that takes the user's permission model and their
database schema, then produces an access report: which tables, columns,
relationships, query shapes, and row regions can be accessed by each user
class, and which cannot.

The goal is not just to lint rules. The goal is to make access behavior
explainable and mechanically defensible before the rules are deployed. The
evaluator should be able to answer questions like:

- Can an unauthenticated user reach any row in `billing_events`?
- Can a member of organization A infer rows owned by organization B through a
  join, foreign key, aggregate, or subscription reuse path?
- Does a query that joins two individually-safe tables create a combined result
  that exposes denied rows, identifiers, counts, or relationship existence?
- Are there schema objects that have no explicit policy?
- Did a rule edit expand access compared with the currently deployed policy?
- Can the runtime prove that a requested subscription is contained within the
  user's permitted region?

## Product Shape

The feature is exposed as an offline evaluator and, later, as a runtime guard.
The eventual implementation should be isolated from the enforcement path in
`palimpsest-permissions`, but this document does not create a new crate yet.

The evaluator consumes:

- A catalog snapshot: schemas, tables, columns, types, primary keys, foreign
  keys, indexes, nullable columns, generated columns, views, and publication
  membership.
- A permission model: existing `permissions.toml` rules, user-context schema,
  rule modes, and future policy annotations.
- Optional role fixtures: representative user-context values or symbolic role
  definitions such as `anonymous`, `org_member`, `org_admin`, and
  `system_admin`.
- Optional query corpus: saved subscriptions or application-declared query
  shapes to evaluate against the policy.

The evaluator produces:

- A machine-readable evaluation report.
- A human-readable access matrix.
- A proof trace for each access decision.
- A diff report comparing two permission models.
- A set of hard failures for policy gaps that cannot be proven safe.

## Component Boundary

The permission evaluation component should own analysis and proof artifacts. It
should not rewrite live queries or enforce production subscriptions directly.

Responsibilities:

- Normalize schema and permission rules into an analysis model.
- Compile predicates into a typed symbolic expression graph.
- Compute table-level, column-level, row-level, and path-level access bounds.
- Prove containment between requested query regions and permitted regions.
- Explain denied, allowed, unknown, and partially-proven access.
- Emit stable JSON reports for CI, CLIs, dashboards, and audit archives.

Non-responsibilities:

- Runtime row filtering. That remains in `palimpsest-permissions`.
- WAL decoding and catalog discovery. Those remain in `palimpsest-wal` and
  server integration code.
- General SQL theorem proving. The evaluator targets Palimpsest's supported SQL
  and permission predicate subset first.

## Core Concepts

### Access Subject

An access subject describes the user class being evaluated. It can be concrete
or symbolic.

Concrete subject:

```toml
[subject]
name = "alice"
user.id = 42
user.org_id = 7
user.is_admin = false
```

Symbolic subject:

```toml
[subject]
name = "org_member"
where = "$user.org_id IS NOT NULL AND $user.is_admin = false"
```

Concrete subjects are useful for smoke tests and support tickets. Symbolic
subjects are useful for proofs because they describe all users matching a role
shape rather than one sampled user.

### Access Region

An access region is a typed predicate over a schema object. For a table, it is
the set of rows a subject may observe. For a column, it is the table region plus
the visible column set. For a query, it is the query result region projected
from the base regions.

Examples:

- `documents`: rows where `tenant_id = $user.tenant_id`
- `documents.body`: readable only inside the visible `documents` row region
- `comments JOIN documents`: readable only where both sides are visible and the
  join predicate preserves tenant containment

### Proof Result

Every evaluated object returns one of four results:

- `ProvenAllowed`: the evaluator can prove the subject can access this region.
- `ProvenDenied`: the evaluator can prove the subject cannot access this
  region.
- `PartiallyAllowed`: some projected columns, rows, or join paths are proven
  accessible and others are denied.
- `Unknown`: the evaluator cannot prove either direction with the supported
  logic.

`Unknown` is a failure in strict CI mode. It is acceptable in exploratory mode
as long as the report includes the unsupported construct or missing schema fact
that caused the gap.

## Analysis Pipeline

### 1. Load and Validate Inputs

The evaluator loads the catalog snapshot and permission configuration, then
performs the same basic validation as `palimpsest-permissions`: known tables,
known columns, known user fields, supported predicate syntax, deterministic
operators, and type-correct comparisons.

Additional validation checks:

- Every published table is classified as explicitly allowed, explicitly denied,
  or intentionally ignored.
- Every referenced foreign key target is present in the catalog snapshot.
- Every view or generated column used by a policy has a known definition.
- Every policy has a stable identifier for diffing and audit trails.

### 2. Build the Schema Graph

The schema graph models tables as nodes and relationships as edges.

Edges include:

- Foreign keys.
- Primary-key and unique-key equivalences.
- View dependencies.
- Query corpus joins.
- Permission predicate references.

Each edge records whether access can flow across it. For example, a
`comments.document_id -> documents.id` edge may be safe only if both tables are
also constrained by the same tenant key. The evaluator should detect when a
join can escape a tenant boundary.

### 3. Compile Permission Predicates

Permission predicates are lowered into a typed symbolic expression graph. The
graph preserves enough structure to reason about implication and contradiction:

- Boolean connectives: `AND`, `OR`, `NOT`.
- Comparisons: `=`, `<>`, `<`, `<=`, `>`, `>=`.
- Null checks.
- Constant-only `IN` lists.
- User-context variables.
- Column references.

The first version should intentionally reuse the predicate subset that the
runtime rewriter already supports. Unsupported predicates are reported as
`Unknown` rather than approximated as allowed.

### 4. Derive Base Access Regions

For each table and subject, the evaluator computes the disjunction of all
matching row-visibility rules. Tables with no rule are reported according to
the configured posture:

- `open_by_default`: match the current runtime behavior and mark the table as
  universally visible.
- `deny_by_default`: mark the table as denied unless a rule grants access.
- `audit_only`: preserve runtime behavior but emit a policy-gap finding.

The report must explicitly state which posture was used.

### 5. Propagate Through Relationships

The evaluator walks the schema graph and checks whether access to one object
can reveal data from another object. This catches indirect access such as:

- A visible child row exposing a parent identifier.
- A join allowing cross-tenant rows because only one side is tenant-filtered.
- An aggregate leaking counts for denied rows.
- A subscription query projecting columns from a table with only subscribe-mode
  rules.

The central claim this phase must support is: a join between accessible regions
does not unexpectedly expand access. It is not enough to prove that each table
has a policy in isolation. The evaluator must prove that each join output row,
projected column, and relationship fact is derived only from rows inside the
subject's allowed region for every base table participating in the query.

Path evaluation should produce a trace:

```text
subject org_member
query documents_with_comments
documents: tenant_id = $user.org_id
comments: document_id -> documents.id
proof: comments rows are contained by visible documents through FK
result: ProvenAllowed
```

### 6. Evaluate Join Safety

For every join in the schema graph or query corpus, the evaluator should
classify the join as `JoinProvenContained`, `JoinDenied`, `JoinPartial`, or
`JoinUnknown`.

The evaluator should check:

- **Both-side containment:** every row from the left and right inputs that can
  contribute to the join output is contained in the subject's allowed region
  for that table.
- **Tenant-key preservation:** if both tables are tenant-scoped, the join
  predicate preserves the tenant boundary or is otherwise constrained by
  equivalent policy predicates.
- **Relationship existence leakage:** the output does not reveal that a denied
  row exists through a matching foreign key, missing foreign key, count, boolean
  flag, or nullable projection.
- **Projection safety:** projected columns from joined tables are visible under
  the policy for the base table that owns each column.
- **Outer-join null semantics:** `LEFT`, `RIGHT`, and `FULL` joins do not leak
  denied-side existence or absence through null-extended rows.
- **Many-to-many safety:** bridge tables are covered by their own policy, and
  traversing the bridge cannot connect the subject to denied rows on either
  side.
- **Aggregate safety:** `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `DISTINCT`, and
  grouped results are computed only over allowed rows, or the result is marked
  `Unknown`.

Examples:

```text
documents policy:
  documents.org_id = $user.org_id

comments policy:
  comments.org_id = $user.org_id

query:
  SELECT documents.title, comments.body
  FROM documents
  JOIN comments ON comments.document_id = documents.id

proof:
  documents rows are limited to $user.org_id
  comments rows are limited to $user.org_id
  join key does not itself prove tenant equivalence
  comments.document_id has FK to documents.id
  comments.org_id is constrained to the same subject value as documents.org_id
result:
  JoinProvenContained
```

```text
documents policy:
  documents.org_id = $user.org_id

comments policy:
  true

query:
  SELECT documents.title, comments.body
  FROM documents
  JOIN comments ON comments.document_id = documents.id

proof:
  documents rows are limited to $user.org_id
  comments rows are globally visible
  no policy or catalog constraint proves comments.org_id equals documents.org_id
  join can project comments.body for rows outside the intended tenant boundary
result:
  JoinDenied or JoinUnknown, depending on catalog constraints
```

The report should distinguish "the join is safe because both inputs are
rewritten and filtered before the join" from "the join is intrinsically safe
under the application's query predicate." Both are useful, but they answer
different audit questions.

### 7. Check Query Containment

For each subscription query in the optional corpus, the evaluator proves whether
the query's base-table regions are contained within the allowed regions for the
subject.

Containment examples:

- Query predicate `documents.tenant_id = 7` is contained by policy predicate
  `documents.tenant_id = $user.org_id` for a concrete subject where
  `$user.org_id = 7`.
- Query predicate `documents.tenant_id = 7` is not contained for a symbolic
  subject with unconstrained `$user.org_id`.
- Query predicate `true` is not contained by `tenant_id = $user.org_id`, but
  the runtime may still be safe because the rewriter adds the policy predicate.
  The evaluator should distinguish "query safe after rewrite" from "query
  intrinsically contained".

### 8. Compare Policy Versions

The evaluator can compare two policy inputs and report expansions or
contractions:

- New table became visible.
- Predicate became weaker.
- Predicate became stronger.
- A denied path became allowed through a new relationship.
- A proof changed from `ProvenAllowed` to `Unknown`.

In CI, policy expansion should require explicit approval or an audit artifact.

## Formal Model

The evaluator models a table `T` as a set of rows. A permission rule for subject
`S` is a predicate `P(T, S)` over rows of `T` and fields of `S`.

The allowed region for table `T` and subject `S` is:

```text
Allowed(T, S) = OR(P1(T, S), P2(T, S), ..., Pn(T, S))
```

If no rules match `T`, the allowed region is determined by the configured
default posture.

A query `Q` is safe for subject `S` when every base-table row that can
contribute to `Q` is contained in that table's allowed region:

```text
RowsUsed(Q, T) => Allowed(T, S)
```

A join `J = A JOIN B ON E` is safe for subject `S` when both contributing row
sets are contained and every projected fact from the join output is derived
from allowed rows:

```text
RowsUsed(J, A) => Allowed(A, S)
RowsUsed(J, B) => Allowed(B, S)
ProjectedFacts(J) => FactsAllowed(A, S) AND FactsAllowed(B, S)
```

For outer joins, the null-extended side is also a fact: it can reveal that a
related row is absent or denied. The evaluator should only prove an outer join
safe when that absence/existence signal is itself allowed or irrelevant to the
projection.

For rewritten runtime queries, the proof target is different:

```text
RowsUsed(Rewrite(Q, Policy), T) => Allowed(T, S)
```

The evaluator should track both. The first tells product and application teams
what the query asked for. The second tells infrastructure teams whether
Palimpsest's rewrite preserves the policy.

## Report Format

The report should be stable JSON with a schema version.

```json
{
  "schema_version": 1,
  "policy_hash": "sha256:...",
  "catalog_hash": "sha256:...",
  "default_posture": "audit_only",
  "subjects": [
    {
      "name": "org_member",
      "objects": [
        {
          "object": "public.documents",
          "kind": "table",
          "result": "ProvenAllowed",
          "region": "tenant_id = $user.org_id",
          "proof": [
            "rule documents_tenant applies",
            "predicate is type-correct",
            "tenant_id is non-null"
          ]
        },
        {
          "object": "documents_with_comments",
          "kind": "join",
          "result": "JoinProvenContained",
          "region": "documents.org_id = $user.org_id AND comments.org_id = $user.org_id",
          "proof": [
            "documents_tenant constrains documents.org_id",
            "comments_tenant constrains comments.org_id",
            "comments.document_id has a foreign key to documents.id",
            "both tenant predicates use the same subject value"
          ]
        },
        {
          "object": "public.billing_events",
          "kind": "table",
          "result": "ProvenDenied",
          "region": "false",
          "proof": ["rule deny_billing applies"]
        }
      ]
    }
  ],
  "findings": [
    {
      "severity": "error",
      "code": "UNPROVEN_PUBLICATION_TABLE",
      "object": "public.audit_log",
      "message": "published table has no explicit permission rule"
    }
  ]
}
```

The human-readable form should prioritize an access matrix:

| Subject | Table | Rows | Columns | Paths | Result |
| --- | --- | --- | --- | --- | --- |
| `org_member` | `documents` | `tenant_id = $user.org_id` | all | contained joins only | allowed |
| `org_member` | `documents JOIN comments` | both sides tenant-contained | projected columns only | FK preserves document relation | allowed |
| `org_member` | `billing_events` | none | none | none | denied |
| `anonymous` | `documents` | none | none | none | denied |

## CLI and CI UX

The first user-facing command should be:

```text
palimpsest evaluate-permissions \
  --catalog catalog.json \
  --permissions permissions.toml \
  --subjects subjects.toml \
  --queries subscriptions.sql \
  --default-posture audit-only \
  --output permissions-report.json
```

Exit behavior:

- Exit `0` when all strict checks are proven.
- Exit `1` for policy gaps, proof regressions, or `Unknown` results in strict
  mode.
- Exit `2` for malformed input.

CI should support a baseline file so teams can review intentional expansions:

```text
palimpsest evaluate-permissions --against main.permissions-report.json
```

## Runtime Integration

The first version is offline. Later, the server can use the same component for
runtime preflight:

1. Compile policy and catalog into an evaluation artifact at startup.
2. For each subscription, ask the evaluator whether the query is intrinsically
   contained, safe after rewrite, denied, or unknown.
3. Emit structured metrics and attach proof identifiers to permission-denied
   errors.

Runtime preflight must be conservative. `Unknown` should fail closed unless the
operator explicitly configures audit-only mode.

## Security Properties

The evaluator should be designed around these claims:

- **Sound denial:** if the report says `ProvenDenied`, the supported model has
  no satisfying row path for the subject.
- **Sound containment:** if the report says a query is contained, every
  contributing base row is inside the subject's allowed region under the
  supported SQL subset.
- **Join non-expansion:** if the report says a join is contained, the join
  cannot introduce rows, projected values, counts, or relationship-existence
  facts outside the subject's allowed regions for the joined tables.
- **Explicit uncertainty:** unsupported SQL, missing catalog facts, and
  incomplete role definitions produce `Unknown`, not a silent allow.
- **Stable auditability:** reports include hashes for the catalog, policy,
  subject definitions, and query corpus used for the proof.

The initial implementation can be incomplete, but it must not claim proof for
constructs outside the modeled subset.

## Implementation Phases

### Phase 1: Report Skeleton

- Define input and output data structures.
- Parse existing permission configs by depending on `palimpsest-permissions`.
- Accept catalog snapshots from the existing conformance/catalog format.
- Emit table-level access matrices.
- Treat unsupported constructs as `Unknown`.

### Phase 2: Predicate Reasoning

- Lower predicates into a symbolic expression graph.
- Implement simple implication and contradiction checks for equality,
  conjunction, disjunction, constants, nullability, and user fields.
- Support concrete subject substitution.
- Produce proof traces for every table-level decision.

### Phase 3: Schema Graph Evaluation

- Add foreign key and view-dependency reasoning.
- Detect cross-tenant joins and unguarded relationship paths.
- Prove that inner joins, outer joins, bridge-table traversals, and aggregate
  query shapes do not expand access beyond base-table policies.
- Evaluate saved subscription queries against allowed regions.
- Emit path-level findings.

### Phase 4: Policy Diffing

- Compare two reports or two policy inputs.
- Classify expansions, contractions, proof regressions, and newly unknown
  regions.
- Add CI-oriented exit codes and baseline approval workflow.

### Phase 5: Runtime Preflight

- Build startup artifacts used by the server.
- Attach proof IDs to subscription accept/deny decisions.
- Add metrics for allowed, denied, unknown, and rewrite-required decisions.

## Open Questions

- Should Palimpsest keep runtime "no rule means allow" semantics while the
  evaluator defaults to audit-only or deny-by-default?
- How should column-level policies be represented when the runtime currently
  focuses on row-level predicates?
- Do symbolic subjects need a first-class policy language, or can they be
  expressed as constrained user-context fixtures?
- Which catalog snapshot format should be the long-term public interface?
- Should aggregate leakage be modeled in v1, or initially reported as
  `Unknown` whenever denied rows can affect aggregate results?
