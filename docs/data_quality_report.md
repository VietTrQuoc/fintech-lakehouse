# Data Quality Report — Silver Transactions (Day 7)

- run_id: `quality_20260624_100555` · silver_run_id: `silver_20260624_100554` · generated_at: 2026-06-24T03:05:55.675114+00:00
- **Overall: PASS** — 22/22 check PASS · 0 ERROR · 0 WARNING

## 1. Summary
- Bronze rows: 10,000 → Silver rows: 10,000 (no row loss ✓)
- Valid: 9,880 (98.80%) · Invalid (quarantine-bound): 120
- Soft orphan FK: 40 → Unknown member (Day 9)

## 2. Integrity checks (ERROR)
| check | observed | threshold | status |
|---|---|---|---|
| rowcount_silver_eq_bronze | 0 | == 0 | PASS |
| schema_conformance | 0 | == 0 | PASS |
| amount_vnd_notnull_when_valid | 0 | == 0 | PASS |
| exchange_rate_notnull_when_valid | 0 | == 0 | PASS |
| exactly_one_survivor_per_group | 0 | == 0 | PASS |
| no_dup_among_valid | 0 | == 0 | PASS |
| bucket_consistency | 0 | == 0 | PASS |
| event_date_in_window | 0 | == 0 | PASS |

## 3. Row-flag rates (WARNING)
| check | observed | threshold | status |
|---|---|---|---|
| rate_null_transaction_id | 0.100% | <= 0.30% | PASS |
| rate_invalid_amount | 0.350% | <= 1.00% | PASS |
| rate_invalid_timestamp | 0.250% | <= 0.75% | PASS |
| rate_invalid_currency | 0.100% | <= 0.30% | PASS |
| rate_invalid_channel | 0.100% | <= 0.30% | PASS |
| rate_invalid_type | 0.000% | <= 0.10% | PASS |
| rate_invalid_status | 0.000% | <= 0.10% | PASS |
| rate_invalid_location | 0.100% | <= 0.30% | PASS |
| rate_duplicate | 0.200% | <= 0.50% | PASS |
| rate_fk_customer_orphan | 0.150% | <= 1.00% | PASS |
| rate_fk_account_orphan | 0.150% | <= 1.00% | PASS |
| rate_fk_merchant_orphan | 0.100% | <= 1.00% | PASS |
| valid_rate | 98.800% | >= 95.00% | PASS |

## 4. Quarantine buckets
| bucket | count |
|---|---|
| quarantine_bad_records | 40 |
| quarantine_duplicate_transactions | 20 |
| quarantine_invalid_amount | 35 |
| quarantine_invalid_timestamp | 25 |

## 5. Top error codes (`_dq_errors`)
| error_code | count |
|---|---|
| invalid_amount | 35 |
| invalid_timestamp | 25 |
| duplicate_transaction_id | 20 |
| fk_customer_orphan | 15 |
| fk_account_orphan | 15 |
| invalid_currency | 10 |
| invalid_channel | 10 |
| null_transaction_id | 10 |
| fk_merchant_orphan | 10 |
| invalid_location | 10 |

## 6. Manifest reconciliation
| error_type | manifest | silver_flagged | coverage |
|---|---|---|---|
| null_transaction_id | 10 | 10 | 100.0% |
| invalid_amount | 35 | 35 | 100.0% |
| invalid_timestamp | 25 | 25 | 100.0% |
| invalid_currency | 10 | 10 | 100.0% |
| invalid_channel | 10 | 10 | 100.0% |
| invalid_location | 10 | 10 | 100.0% |
| orphan_customer | 15 | 15 | 100.0% |
| orphan_account | 15 | 15 | 100.0% |
| orphan_merchant | 10 | 10 | 100.0% |
| duplicate_transaction_id | 20 | 20 | 100.0% |

## 7. Kết luận
Đủ điều kiện sang Day 8 (quarantine split): **CÓ**.
