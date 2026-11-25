# Retail Lakehouse Design and Release Plan

**Status** — Draft for design review. The named roles describe who would approve each decision in a company project. For this reference implementation, the project owner records decisions and verifies the evidence.

Finance needs a reconciled daily view of sales and refunds. Fulfillment needs to identify late shipments and see the latest known carrier status. Data support needs to diagnose failed loads and replay them safely. The public implementation uses synthetic data and a single Azure development environment.

## Scope and release order

- **Release 1 · Batch reporting.** Python source generator, configuration-driven ADF ingestion, ADLS Gen2, PySpark and Delta on Azure Databricks, Synapse serverless SQL views, and a Power BI report. The release includes historical backfill, quarantine, reconciliation, and a support runbook.
- **Release 2 · Shipment streaming.** Event Hubs and Databricks Structured Streaming with a durable checkpoint, duplicate handling, late-event protection, and reconciliation against a daily carrier snapshot. Power BI remains refresh-based. Measure event processing and report refresh separately.
- **Company deployment.** Define separate test and production boundaries, identities, deployment approval, support ownership, and service objectives using the company’s operating requirements.

## Design decisions for review

1. **Finance owner · Revenue recognition.** Gross sales are captured payment amounts in INR, excluding cancelled or uncaptured transactions. Net sales subtract successful refunds on their posting date. Refund reports retain the original payment and order references. Confirm whether tax, shipping, and discounts belong in this measure before using it for company reporting.
2. **Operations owner · Delivery metrics.** Delivered shipments are on time when actual delivery is at or before the promised timestamp. The denominator is delivered shipments with both timestamps. Open overdue shipments appear separately. Store event timestamps in UTC and confirm the business timezone before adopting any calendar-day reporting in a company deployment.
3. **Source system owners · Data delivery.** The lab uses 30 days and approximately 10,000 synthetic orders. Each daily feed has a schema version, arrival expectation, unique key, and explicit batch date. Distinguish a valid empty batch from a missing file. For production, agree on correction, retention, and escalation rules with each owner.
4. **Engineering owner · Streaming ordering.** Deduplicate by event ID. Update current shipment status only when the incoming source ordering key is newer. Use event time with a documented tie-breaker in the simulator. Prefer a reliable source sequence when one exists. Keep raw events for reconciliation.
5. **Platform owner · Service readiness.** Verify subscription credit, regional access, Databricks compute availability, Event Hubs connectivity, and Power BI publishing access before committing to a deployment date. Confirm company targets for freshness, recovery time, and retention during design review.

## Data design

- **Orders and order items.** One order per order ID and one item per order ID plus line ID. The order-item fact supports product and seller analysis.
- **Payments and refunds.** One record per transaction ID. A refund references the original payment. Compute finance totals at transaction grain. Use an explicit allocation rule if the business requires item-level finance measures.
- **Shipments and carrier events.** One current record per shipment ID and one event per event ID. An order may have multiple shipments. The daily carrier snapshot has one row per shipment per snapshot date.
- **Dimensions.** Date, product, seller, and geography provide consistent filters. Document whether a change replaces a dimension value or preserves history before implementing historical reporting.
- **Audit and quarantine.** Record run ID, feed, batch date, file count, accepted and rejected counts, status, and error details. Retain rejected records with a reason, source reference, and run ID.

## Processing and serving

1. Python generates deterministic clean and defective fixtures with a fixed seed. Feed contracts and expected reconciliation totals live with the code.
2. ADF reads feed configuration, lands input files in dated raw paths, and calls Databricks with the feed, batch date, and run ID. A backfill submits an explicit range of dates. Preserve raw input for replay.
3. Databricks builds Bronze from the original records, Silver with validated and deduplicated Delta tables, and Gold with business facts and dimensions. Recompute affected reporting partitions and reconcile counts and financial totals before promoting the output.
4. Publish validated Gold Parquet exports to versioned storage paths. Point Synapse serverless SQL reporting views at a confirmed export version. Restrict reporting access to Gold data. Use managed identities and environment-specific settings for Azure connections.
5. Power BI imports the Synapse views. Build Executive Revenue, Fulfillment Operations, and Data Reliability pages. Show the last successful refresh and reconcile the report against a fixed SQL test period.
6. In Release 2, Databricks reads Event Hubs using the Kafka-compatible endpoint. Store the stream checkpoint on durable storage and use idempotent writes when applying updates. Compare the event-derived shipment state with the daily carrier snapshot and log differences.

## Acceptance tests and evidence

- **T01 Batch and backfill.** Run two different dates and a three-day backfill. Verify the input paths, run audit records, and expected output totals.
- **T02 Replay.** Run the same date twice. Verify uniqueness at each documented business grain and unchanged totals for identical input.
- **T03 Data quality.** Inject a malformed record and a missing file. Verify quarantine records the malformed row and the missing-feed run fails with a clear error.
- **T04 Finance reconciliation.** Load a multi-item order with multiple payment or refund transactions and a refund on a later date. Verify expected gross sales, refunds and net sales in PySpark, Synapse and Power BI.
- **T05 Reporting release.** Confirm Power BI values equal the Synapse views for the same period and filters. Capture report refresh time.
- **T06 Stream recovery.** Send a duplicate and an out-of-order event. Restart using the same checkpoint. Verify one logical event per event ID, a current shipment state that reflects the newest event, and reconciliation against the snapshot.
- **T07 Release recovery.** Rehearse reverting reporting views to the previous validated export and refreshing the report. Record what operators do if ingestion is healthy but reporting output is wrong.

## Release checklist

1. Review the GitHub pull request and record the release commit. Run generator tests, schema checks, and transformation tests on fixed fixtures.
2. Deploy environment configuration, identities, ADF resources, Databricks jobs, and Synapse SQL definitions. Verify that scheduled triggers are controlled during deployment. Start with a manual smoke run.
3. Run the acceptance tests for the release. Finance reviews reconciliation, operations reviews delivery metrics, and data support rehearses restart and replay. For this lab, record the results against each of those roles.
4. Promote the validated Gold export, refresh Power BI, and record the published view version, run IDs, refresh time, and known issues.
5. If a release fails, stop the affected trigger, restore the prior deployed definitions as needed, and point reporting back to the last validated export. Preserve source data and audit records. Reconcile before replaying and reopening the trigger.

## First implementation milestone

Write the source contracts and Python generator before provisioning new cloud services. Deliver a clean two-day fixture and defect fixtures for duplicates, late shipment events, malformed data, and a refund posted after the sale. Include expected counts and revenue totals. This evidence becomes the input for the first ADF and Databricks integration tests.

For another company project, reuse the scope review, source contracts, acceptance tests, release checklist, and recovery rehearsal. Replace the retail definitions, sample data, owners, service objectives, and environment configuration with the company’s approved values.
