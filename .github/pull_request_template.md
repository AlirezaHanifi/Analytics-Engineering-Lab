## Summary

<!-- Describe the domain pipeline completed in this branch -->

## Verification Checklist

- [ ] Code tested locally against the pinned Docker environment.
- [ ] Airflow DAG runs successfully without task failures.
- [ ] ClickHouse tables created and populated correctly.
- [ ] Metabase dashboard built and validated.

## Review Workflow

- [ ] Feature branch created for this work.
- [ ] PR opened against `prod`.
- [ ] Reviewer assigned.
- [ ] Reviewer approved the PR.
- [ ] Alireza assigned for final review.
- [ ] Alireza approved before merge.

## Notes

- Direct pushes to `prod` are not allowed.
- `prod` must remain protected and only accept merged PRs.
