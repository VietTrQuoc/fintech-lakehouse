# Phân tích cơ bản trên Bronze (Day 4)

> Tự sinh bởi `src/analytics/run_basic_analytics.py` lúc 2026-06-18 15:06:17.
> Nguồn: `sql/basic_analytics.sql` chạy trực tiếp trên Bronze parquet bằng DuckDB.

## Q0 - Data quality peek (đếm bad rows còn sót lại trong Bronze)

*1 dòng*

```
 total_rows  bad_amount  bad_timestamp  empty_txn_id
    2000000      4666.0         2500.0        2000.0
```

## Q1 - Tổng giao dịch và giá trị theo ngày (+ running total bằng Window)

*15 dòng*

```
  txn_date  txn_count  total_amount_vnd  running_total_vnd
2025-12-13      10439      4.184249e+10       4.184249e+10
2025-12-14      10754      4.528095e+10       8.712344e+10
2025-12-15      12035      5.126090e+10       1.383843e+11
2025-12-16      12159      4.804795e+10       1.864323e+11
2025-12-17      12420      5.331695e+10       2.397492e+11
2025-12-18      12619      5.204137e+10       2.917906e+11
2025-12-19      12482      5.647789e+10       3.482685e+11
2025-12-20       9787      4.045675e+10       3.887252e+11
2025-12-21       9950      3.931565e+10       4.280409e+11
2025-12-22      12698      5.412856e+10       4.821695e+11
2025-12-23      12638      5.150792e+10       5.336774e+11
2025-12-24      12462      4.794107e+10       5.816184e+11
2025-12-25      12220      4.973337e+10       6.313518e+11
2025-12-26      12221      4.822657e+10       6.795784e+11
2025-12-27      10664      4.206047e+10       7.216389e+11
```

## Q2 - Kênh giao dịch phổ biến nhất (count, giá trị, % share)

*6 dòng*

```
channel  txn_count  total_amount_vnd  pct_of_txn
 mobile     876972      4.919168e+12       44.00
    atm     649128      2.454922e+12       32.57
     qr     204413      6.635865e+10       10.26
    pos     137235      4.333084e+10        6.89
    web     123252      6.939390e+11        6.18
    foo       2000      6.906760e+09        0.10
```

## Q3 - Top 10 merchant theo doanh số (JOIN lấy tên + RANK Window)

*10 dòng*

```
 rnk merchant_id                                merchant_name  category  txn_count  total_amount_vnd
   1  mer_001155                   Bùi Nguyễn Công ty Cổ phần utilities     109625      5.364446e+10
   2  mer_001483                      Phạm Hoàng Công ty TNHH ecommerce      51436      2.790505e+10
   3  mer_000396                      Mai và đối tác Tập Đoàn   fashion      24390      1.810893e+10
   4  mer_000250                                 Mai Tập Đoàn ecommerce      33314      1.512351e+10
   5  mer_001849                Đặng và Trần Công ty Hợp danh    gaming      19219      9.883965e+09
   6  mer_001089                 Đặng và đối tác Công ty TNHH ecommerce      11423      7.400854e+09
   7  mer_000076 Hoàng và đối tác Công ty Trách nhiệm hữu hạn transport      15730      6.542467e+09
   8  mer_000979                Vũ và Nguyễn Công ty TNHH MTV    gaming      13246      5.591893e+09
   9  mer_000572    Đặng và Hoàng Công ty Trách nhiệm hữu hạn   grocery      10200      3.890822e+09
  10  mer_001217                             Mai Công ty TNHH      food       3628      3.747294e+09
```

## Q4 - Giao dịch có amount bất thường so với lịch sử KH (z-score bằng Window)

*15 dòng*

```
customer_id   transaction_id transaction_type   amount_vnd  amount_zscore
 cus_006098 txn_000000793489         transfer 6510488269.0         239.55
 cus_003329 txn_000000625635         transfer 2352267186.0         161.12
 cus_014937 txn_000000149399         transfer 2090815378.0         136.28
 cus_019140 txn_000001461263       withdrawal 2347545996.0         130.11
 cus_008771 txn_000001576670         transfer 2532223295.0         122.84
 cus_008771 txn_000000183864       withdrawal 2291887858.0         111.16
 cus_014937 txn_000001116167       withdrawal 1467406476.0          95.57
 cus_014937 txn_000000615559         transfer 1251045390.0          81.44
 cus_015025 txn_000000158633         transfer 1257270087.0          79.65
 cus_014937 txn_000000996315       withdrawal 1188195116.0          77.33
 cus_014937 txn_000000489709         transfer 1122432120.0          73.04
 cus_001233 txn_000001101740         transfer 1045568651.0          71.12
 cus_014937 txn_000001006039         transfer 1091047833.0          70.99
 cus_003329 txn_000000747774       withdrawal 1008093198.0          68.90
 cus_013180 txn_000001999287         transfer 1022396269.0          68.16
```

## Q5 - Tỷ lệ giao dịch thất bại theo nhóm merchant (feed mart_merchant_risk)

*10 dòng*

```
   category  txn_count  fail_pct
     gaming      59983      5.96
     travel      40136      5.14
electronics      18240      4.69
  ecommerce     160738      4.40
    fashion      68352      4.23
       food      58303      3.62
  transport      49593      3.22
  education      22524      3.21
  utilities     137044      3.14
    grocery      73923      2.99
```
