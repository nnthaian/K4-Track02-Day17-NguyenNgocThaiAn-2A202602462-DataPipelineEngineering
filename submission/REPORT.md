# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

- **Họ tên / MSSV:** Nguyễn Ngọc Thái An (2A202602462)
- **Repo:** https://github.com/nnthaian/K4-Track02-Day17-NguyenNgocThaiAn-2A202602462-DataPipelineEngineering
- **Commit bài nộp:** 
- **AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Codex hỗ trợ đọc, feedback và cập nhật report.
- **Nguồn tham khảo:** README, hướng dẫn và mã nguồn của repo; không dùng nguồn ngoài.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Verify thấy 24 hàng cho 12 ticket; T-91 có 3 trạng thái thay vì chỉ `high / closed / bug`. | Feature không khớp full recompute; u05 ngày 2026-08-12 có `(events, clicks, feedback_down) = (2, 1, 0)` thay vì `(5, 3, 1)`. P99 = 3 ngày nhưng lookback = 0. | T-97 vẫn có 2 hàng chưa bị đánh dấu xoá và còn dữ liệu cá nhân ở Silver; snapshot mới nhất còn 1 hàng, RAG còn 2 chunks của T-97. |
| **Nguyên nhân gốc** | Chỉ dedup trong batch, sau đó INSERT nối thêm qua các batch; chưa so LSN với trạng thái đang lưu. | Lookback = 0 chỉ tính lại ngày ingest; event trễ 3 ngày đã vào Silver nhưng partition ngày xảy ra không được tính lại. | Staging chỉ lấy khoá từ `after`; delete có `after=null` nên bị lọc mất bởi `ticket_id IS NOT NULL`. |
| **Cách sửa** (file, vài dòng) | `pipeline/silver.py`: MERGE theo `ticket_id`; INSERT khi chưa có khoá, UPDATE chỉ khi `s._lsn > t._lsn`. | `pipeline/config.py`: đặt `LOOKBACK_DAYS = 3 = ceil(P99)` đo từ Bronze; giữ logic gom nhóm theo event time và overwrite partition trong Gold. | `pipeline/staging.py`: với `op=d`, lấy khoá từ `before`, dự phòng từ Kafka key; giữ LSN và đọc PII từ `after=null` để Silver xoá các trường đó. |
| **Khái niệm trên slide** | Silver có khoá; keyed upsert và LSN guard giúp replay idempotent, trạng thái mới nhất thắng. | Event time là lúc sự kiện xảy ra; ingest time là lúc dữ liệu đến. Lookback tính lại partition cũ để nhận event muộn đúng ngày. | CDC delete phải lan xuống Gold; tombstone Silver giữ LSN chống hồi sinh khi replay, khác Kafka tombstone `value=null` không mang thay đổi mới. |

## 2. Các con số

- Lateness đo từ 43 bản ghi Bronze (đơn vị ngày lịch): **P50 = 0.00, P95 = 2.90, P99 = 3.00**, max = 3 ngày → đã sửa `LOOKBACK_DAYS = ceil(P99) = 3` (ban đầu là `0`).
- `submission/checksums.txt`: **PASS** — C0 = C1 = C2 = C3 = `39e115c510ecdf526800eac227158a4f` sau khi sửa đủ ba lỗi.
- dbt build: **PASS=19, WARN=0, ERROR=0**; parity: **PARITY** — Python và dbt cho cùng checksum trên `silver_tickets` và `gold_feature_daily`.

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets` để giữ trạng thái mới nhất; overwrite-partition cho `gold_feature_daily` để tính lại tổng hợp từ Silver đã dedup, nhận event muộn mà không cộng trùng khi replay.
- Lookback 3 ngày theo P99 Bronze: run 08-15 tính lại 08-12..08-15 (4 ngày kể cả ngày hiện tại), nên event xảy ra 08-12 được cập nhật đúng partition; cửa sổ lớn hơn tốn thêm xử lý, event ngoài cửa sổ cần backfill.
- Tombstone thay vì xoá hẳn hàng trong Silver: giữ khoá và LSN của delete để batch cũ không chèn lại ticket; các trường `user_id`, `subject`, `body` được đặt NULL.
- Snapshot dựng từ Bronze “as of” ngày đó để tái lập dữ liệu training và tránh dùng thông tin tương lai; version mới nhận dữ liệu muộn, version cũ giữ kết quả đã dùng.
- DuckDB phù hợp seed nhỏ, chạy SQL tại máy không cần cluster; dbt thêm model, contract và tests. Spark có chi phí vận hành chưa cần thiết cho quy mô lab.

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot bất biến và yêu cầu xoá:** Lab giữ snapshot cũ để tái lập, nên chưa xoá hết dữ liệu T-97. Trong hệ thống thật, cần quy trình xoá ngoại lệ: chặn sử dụng snapshot bị ảnh hưởng, truy vết và xoá/khử định danh dữ liệu ở Bronze, snapshots, index, cache và bản sao lưu theo chính sách; dựng version sạch, đánh dấu version cũ đã thu hồi. Giữ audit không chứa PII và đánh giá dataset/model đã dùng dữ liệu đó. Đánh đổi là mất khả năng tái lập nguyên trạng bản cũ.
2. **Chốt PII:** Đặt bước phát hiện trước khi dữ liệu rời Bronze sang Silver, kết hợp regex với NER tiếng Việt để nhận tên người; trường hợp nghi ngờ đưa vào quarantine và review. Kiểm tra lại trước khi xuất Gold/training/RAG, hạn chế truy cập Bronze. Đo precision/recall theo loại PII trên bộ mẫu gán nhãn, ưu tiên giảm bỏ sót; kiểm tra tên có dấu, biến thể email/điện thoại và theo dõi tỷ lệ PII còn lọt xuống Gold.

## 5. Output (dán nguyên văn)

### Baseline trước khi sửa — 2026-10-05

Đây là kết quả chạy lại để lưu bằng chứng ban đầu, chưa phải kết quả hoàn tất bài nộp.
Output nguyên văn được lưu trong các file bên dưới; mã thoát được lưu riêng trong `baseline/*.exitcode.txt`.

| Lệnh PowerShell | Kết quả thực tế | Output nguyên văn |
|---|---|---|
| `.\.venv\Scripts\python.exe -m scripts.verify` | 8/18 pass, 10 check fail; exit code 1 | [verify.txt](baseline/verify.txt) |
| `.\.venv\Scripts\python.exe -m pytest` | 9 failed, 25 passed; exit code 1 | [pytest.txt](baseline/pytest.txt) |
| `.\.venv\Scripts\python.exe main.py --lateness` | P50 = 0.00, P95 = 2.90, P99 = 3.00, max = 3 ngày; exit code 0 | [lateness.txt](baseline/lateness.txt) |

Verify cũng chạy kiểm tra rerun và sinh `submission/checksums.txt`; kết quả baseline là FAIL.
Bản sao được giữ tại [baseline/checksums.txt](baseline/checksums.txt), còn file có sẵn trước lần verify này được giữ tại [baseline/checksums-before-verify.txt](baseline/checksums-before-verify.txt).

#### Ghi chú CP1: các check fail và triệu chứng quan sát được

Verify có **10 check fail / 18 check**. Bảng dưới ghi nhận hiện tượng từ baseline, chưa kết luận nguyên nhân hay cách sửa.

| # | Check fail | Triệu chứng thực tế |
|---|---|---|
| 1 | Silver: một hàng cho mỗi `ticket_id` | Có 24 hàng cho 12 ticket, xuất hiện nhiều hàng cùng khoá. |
| 2 | Silver: T-91 chỉ giữ trạng thái mới nhất | Có cả `low/open/None`, `high/open/None`, `high/closed/bug` thay vì chỉ trạng thái cuối. |
| 3 | Silver: T-97 thành tombstone và không còn dữ liệu cá nhân | Còn 2 hàng có `is_deleted=False`, `user_id=u06`, subject và body chưa được xoá. |
| 4 | Gold: feature daily khớp full recompute từ Silver | Checksum hiển thị khác nhau: `c50b8851affe != 8630e04a61d1`. |
| 5 | Gold: event trễ của u05 được tính vào ngày 2026-08-12 | Verify thấy `(n_events, n_feedback_down) = (2, 0)` thay vì `(5, 1)`; pytest ghi thêm `n_clicks=1` thay vì `3`. |
| 6 | Gold: lookback bao phủ P99 lateness | `LOOKBACK_DAYS=0`, nhỏ hơn P99 đo được là 3 ngày. |
| 7 | Gold: snapshot training mới nhất loại T-97 | Vẫn còn 1 hàng của T-97 trong snapshot mới nhất. |
| 8 | Gold: thao tác xoá lan đến RAG index | Vẫn còn 2 chunks của T-97. |
| 9 | Gold: mỗi chunk chỉ có một hàng và rerun không embed mới | Có 22 hàng nhưng chỉ 9 chunk phân biệt; số embed mới bằng 0 đã đạt, phần tính duy nhất chưa đạt. |
| 10 | Rerun: checksum Gold sau 3 lần chạy lại ngày 2026-08-12 bằng fresh build | Kiểm tra trả FAIL; checksum từng lần được lưu trong `baseline/checksums.txt`. |

Pytest xác nhận **9 failed, 25 passed**; danh sách test và assertion chi tiết nằm trong output bên dưới.
Lateness trên **43 bản ghi Bronze**: **P50 = 0.00 ngày, P95 = 2.90 ngày, P99 = 3.00 ngày**, tối đa 3 ngày. Đây là số đo baseline để chọn cửa sổ lookback ở bước sửa tiếp theo.

<details>
<summary>Output baseline: verify</summary>

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [XX ] Silver  silver_tickets has exactly one row per ticket_id  (24 rows for 12 tickets)
  [XX ] Silver  T-91 shows its latest state: high / closed / bug  (got [('low', 'open', None), ('high', 'open', None), ('high', 'closed', 'bug')])
  [XX ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left  (got [(False, 'u06', 'Yêu cầu xoá tài khoản', 'Tôi là Nguyễn Văn An, email <EMAIL>, sđt <PHONE>. Xin xoá toàn bộ dữ liệu của tôi.'), (False, 'u06', 'Yêu cầu xoá tài khoản', 'Tôi là Nguyễn Văn An, email <EMAIL>, sđt <PHONE>. Xin xoá toàn bộ dữ liệu của tôi.')])
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [XX ] Gold    gold_feature_daily reconciles with a full recompute from Silver  (c50b8851affe != 8630e04a61d1)
  [XX ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12  (got (2, 0), expected (5, 1))
  [XX ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)  (LOOKBACK_DAYS=0 < 3)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [XX ] Gold    latest training snapshot excludes the deleted ticket T-97  (1 row(s))
  [XX ] Gold    deletes propagate to the RAG index: no chunk of T-97  (2 chunk(s))
  [XX ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks  (22 rows / 9 chunks, embedded 0)
  [XX ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build  (see submission/checksums.txt)

RESULT: 8/18 checks — FAILURES ABOVE
re-run checksums written to submission/checksums.txt
```

</details>

<details>
<summary>Output baseline: pytest</summary>

```text
$ .\.venv\Scripts\python.exe -m pytest
.FFF..FFF..FF...........F.........                                       [100%]
================================== FAILURES ===================================
___________________ test_silver_tickets_one_row_per_ticket ____________________

built = (<_duckdb.DuckDBPyConnection object at 0x000001C019C9D370>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_silver_tickets_one_row_per_ticket(built):
        con, _ = built
        n, n_ids = con.execute("SELECT count(*), count(DISTINCT ticket_id) FROM silver_tickets").fetchone()
>       assert n == n_ids == 12
E       assert 24 == 12

tests\test_contracts.py:34: AssertionError
____________________ test_silver_tickets_latest_state_wins ____________________

built = (<_duckdb.DuckDBPyConnection object at 0x000001C019C9D370>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_silver_tickets_latest_state_wins(built):
        con, _ = built
>       assert rows(con, "SELECT priority, status, category FROM silver_tickets "
                         "WHERE ticket_id = 'T-91'") == [("high", "closed", "bug")]
E       AssertionError: assert [('low', 'ope...osed', 'bug')] == [('high', 'closed', 'bug')]
E         
E         At index 0 diff: ('low', 'open', None) != ('high', 'closed', 'bug')
E         Left contains 2 more items, first extra item: ('high', 'open', None)
E         Use -v to get more diff

tests\test_contracts.py:39: AssertionError
______________________ test_cdc_delete_becomes_tombstone ______________________

built = (<_duckdb.DuckDBPyConnection object at 0x000001C019C9D370>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_cdc_delete_becomes_tombstone(built):
        con, _ = built
>       assert rows(con, "SELECT is_deleted, user_id, subject, body FROM silver_tickets "
                         "WHERE ticket_id = 'T-97'") == [(True, None, None, None)]
E       AssertionError: assert [(False, 'u06...ệu của tôi.')] == [(True, None, None, None)]
E         
E         At index 0 diff: (False, 'u06', 'Yêu cầu xoá tài khoản', 'Tôi là Nguyễn Văn An, email <EMAIL>, sđt <PHONE>. Xin xoá toàn bộ dữ liệu của tôi.') != (True, None, None, None)
E         Left contains one more item: (False, 'u06', 'Yêu cầu xoá tài khoản', 'Tôi là Nguyễn Văn An, email <EMAIL>, sđt <PHONE>. Xin xoá toàn bộ dữ liệu của tôi.')
E         Use -v to get more diff

tests\test_contracts.py:45: AssertionError
______________ test_feature_daily_reconciles_with_full_recompute ______________

built = (<_duckdb.DuckDBPyConnection object at 0x000001C019C9D370>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_feature_daily_reconciles_with_full_recompute(built):
        con, _ = built
>       assert query_checksum(con, "SELECT * FROM gold_feature_daily") == \
            query_checksum(con, feature_daily_full_recompute_sql())
E       AssertionError: assert 'c50b8851affe...824e65099d9be' == '8630e04a61d1...7a49e148926b0'
E         
E         - 8630e04a61d10aa963b7a49e148926b0
E         + c50b8851affeb418fcb824e65099d9be

tests\test_contracts.py:67: AssertionError
__________________ test_late_events_land_in_their_event_day ___________________

built = (<_duckdb.DuckDBPyConnection object at 0x000001C019C9D370>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_late_events_land_in_their_event_day(built):
        con, _ = built
>       assert rows(con, """SELECT n_events, n_clicks, n_feedback_down FROM gold_feature_daily
                            WHERE user_id = 'u05' AND event_date = DATE '2026-08-12'""") == [(5, 3, 1)]
E       assert [(2, 1, 0)] == [(5, 3, 1)]
E         
E         At index 0 diff: (2, 1, 0) != (5, 3, 1)
E         Use -v to get more diff

tests\test_contracts.py:73: AssertionError
___________________ test_lookback_covers_measured_lateness ____________________

built = (<_duckdb.DuckDBPyConnection object at 0x000001C019C9D370>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_lookback_covers_measured_lateness(built):
        con, _ = built
>       assert config.LOOKBACK_DAYS >= lateness_profile(con)["p99_days"]
E       assert 0 >= 3
E        +  where 0 = config.LOOKBACK_DAYS

tests\test_contracts.py:79: AssertionError
_________________ test_deleted_ticket_leaves_training_and_rag _________________

built = (<_duckdb.DuckDBPyConnection object at 0x000001C019C9D370>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_deleted_ticket_leaves_training_and_rag(built):
        con, _ = built
>       assert rows(con, """SELECT count(*) FROM gold_training_set
                            WHERE snapshot_version = 'v2026-08-16' AND ticket_id = 'T-97'""") == [(0,)]
E       assert [(1,)] == [(0,)]
E         
E         At index 0 diff: (1,) != (0,)
E         Use -v to get more diff

tests\test_contracts.py:98: AssertionError
___________________________ test_doc_chunks_unique ____________________________

built = (<_duckdb.DuckDBPyConnection object at 0x000001C019C9D370>, {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chu...3', 'gold_feature_daily': 'c50b8851affeb418fcb824e65099d9be', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})

    def test_doc_chunks_unique(built):
        con, _ = built
        n, n_distinct = con.execute("""SELECT count(*), count(DISTINCT (ticket_id, chunk_idx))
                                       FROM gold_doc_chunks""").fetchone()
>       assert n == n_distinct == 8
E       assert 22 == 9

tests\test_contracts.py:107: AssertionError
_____________ test_rerun_old_day_three_times_keeps_gold_checksum ______________

sandbox = WindowsPath('C:/Users/ADMIN/AppData/Local/Temp/pytest-of-ADMIN/pytest-14/lab171')

    def test_rerun_old_day_three_times_keeps_gold_checksum(sandbox):
        from scripts.rerun_check import rerun_check
        res = rerun_check(config.RERUN_DAY, write=False, quiet=True)
>       assert res["ok"], res["runs"]
E       AssertionError: [('fresh build', {'gold': 'f90edc98a9c0d5c6f5ef168dd422fe88', 'gold_doc_chunks': 'b49150795e84b6016aa19297b2707a03', '...', 'gold_feature_daily': '21d1035eba8aaa7a60e7360302f33519', 'gold_training_set': 'bd80ed585cda944fc9f9d63a63f00316'})]
E       assert False

tests\test_rerun.py:15: AssertionError
=========================== short test summary info ===========================
FAILED tests/test_contracts.py::test_silver_tickets_one_row_per_ticket - asse...
FAILED tests/test_contracts.py::test_silver_tickets_latest_state_wins - Asser...
FAILED tests/test_contracts.py::test_cdc_delete_becomes_tombstone - Assertion...
FAILED tests/test_contracts.py::test_feature_daily_reconciles_with_full_recompute
FAILED tests/test_contracts.py::test_late_events_land_in_their_event_day - as...
FAILED tests/test_contracts.py::test_lookback_covers_measured_lateness - asse...
FAILED tests/test_contracts.py::test_deleted_ticket_leaves_training_and_rag
FAILED tests/test_contracts.py::test_doc_chunks_unique - assert 22 == 9
FAILED tests/test_rerun.py::test_rerun_old_day_three_times_keeps_gold_checksum
9 failed, 25 passed in 7.63s
```

</details>

<details>
<summary>Output baseline: lateness</summary>

```text
$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 0
```

</details>

### Sau mục 2 — sửa khoá Silver

- Chạy `.\.venv\Scripts\python.exe -m pytest`: **27 passed, 7 failed** (baseline: 25 passed, 9 failed). Hai test về ticket duy nhất và trạng thái mới nhất đã pass. [Output thực tế](step2/pytest.txt).
- Kiểm chứng riêng trong warehouse tạm: có 12 ticket duy nhất; T-91 là `high / closed / bug`. So sánh toàn bộ hàng Silver trước/sau replay ngày 08-12 ba lần, ngày 08-14 (LSN cũ), ngày 08-16 (LSN bằng nhau): dữ liệu không đổi. [Kết quả](step2/silver-replay.txt).
- Các test còn fail liên quan late data, CDC delete và hệ quả ở Gold/rerun; chưa hoàn tất các mục đó. Với warehouse cũ đã có hàng trùng, cần fresh build bằng `main.py` để dựng lại Silver từ Bronze với logic mới.

### Sau mục 3 — xử lý dữ liệu đến muộn

Đã đặt `LOOKBACK_DAYS = 3` theo P99 Bronze. `pipeline/gold.py` đã có logic đúng: xoá rồi dựng lại các partition trong `[day - LOOKBACK_DAYS, day]` từ Silver, gom nhóm theo ngày của `event_time`.

| Kiểm chứng | Kết quả | Output thực tế |
|---|---|---|
| `.\.venv\Scripts\python.exe main.py --lateness` | 43 records; P50 = 0.00, P95 = 2.90, P99 = 3.00 ngày; lookback = 3 | [lateness.txt](step3/lateness.txt) |
| `.\.venv\Scripts\python.exe -m pytest` | 31 passed, 3 failed; các test late data và rerun đã pass | [pytest.txt](step3/pytest.txt) |
| `.\.venv\Scripts\python.exe -m scripts.verify` | 15/18 pass; 3 check còn fail về CDC delete | [verify.txt](step3/verify.txt) |
| Truy vấn feature u05 ngày 2026-08-12 và so checksum với full recompute | 5 events, 3 clicks, 1 feedback down; checksum bằng nhau | [feature-check.txt](step3/feature-check.txt) |

Checksum feature và full recompute đều là `8630e04a61d10aa963b7a49e148926b0`. Verify đã sinh lại `submission/checksums.txt` với rerun PASS; bản sao tại [step3/checksums.txt](step3/checksums.txt). Đây là kết quả trung gian: CDC delete chưa sửa nên chưa đạt toàn bộ contract.

### Sau mục 4 — CDC delete và rerun

`op=d` của T-97 nay đi qua staging, cập nhật Silver thành tombstone và được logic Gold hiện có loại khỏi snapshot mới nhất cùng RAG index. Kafka tombstone `value=null` vẫn bị bỏ qua ở staging; Bronze giữ bản ghi gốc.

- Verify: **18/18 — ALL PASS**; pytest: **34 passed**.
- Rerun ngày 2026-08-12 ba lần: **PASS**, checksum của fresh build và cả ba lần chạy lại bằng nhau.
- Sau replay, T-97 có `(is_deleted, user_id, subject, body, _lsn) = (True, None, None, None, 24020000)`; snapshot mới nhất có 0 hàng và RAG có 0 chunks của T-97. [Bằng chứng truy vấn](step4/delete-check.txt).
- Log gốc: [verify](step4/verify.txt), [pytest](step4/pytest.txt), [rerun](step4/rerun.txt); checksum bài nộp: [checksums.txt](checksums.txt). Snapshot cũ vẫn bất biến theo contract của lab.

<details>
<summary>Output step 4: verify</summary>

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt
```

</details>

<details>
<summary>Output step 4: pytest</summary>

```text
$ .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 7.55s
```

</details>

<details>
<summary>Output step 4: rerun</summary>

```text
$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

</details>


### Checkpoint 5 — dbt và parity

Kết quả dưới đây do học viên chạy và cung cấp từ terminal.

#### 1. Output dbt build

Lệnh chạy trong thư mục `dbt_project`: `..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17`.

Output đầy đủ do học viên cung cấp, từ lúc dbt bắt đầu chạy đến dòng tổng kết; gồm 5 models, 13 data tests và 1 unit test.

```text
04:24:13  Running with dbt=1.12.5
04:24:14  Registered adapter: duckdb=1.11.0
04:24:15  Unable to do partial parsing because saved manifest not found. Starting full parse.
04:24:23  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
04:24:23  
04:24:23  Concurrency: 1 threads (target='dev')
04:24:23  
04:24:29  1 of 19 START sql view model main.stg_events ................................... [RUN]
04:24:29  1 of 19 OK created sql view model main.stg_events .............................. [OKin 0.20s]
04:24:29  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
04:24:29  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OKin 0.06s]
04:24:29  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
04:24:30  3 of 19 OK created sql incremental model main.silver_events .................... [OKin 0.28s]
04:24:30  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
04:24:30  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.37s]
04:24:30  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
04:24:30  8 of 19 OK created sql incremental model main.silver_tickets ................... [OKin 0.28s]
04:24:30  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
04:24:30  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.10s]
04:24:30  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
04:24:30  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.04s]
04:24:30  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
04:24:30  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.06s]
04:24:30  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
04:24:30  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.04s]
04:24:30  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
04:24:31  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.04s]
04:24:31  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
04:24:31  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.04s]
04:24:31  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
04:24:31  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.04s]
04:24:31  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
04:24:31  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.04s]
04:24:31  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
04:24:31  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.05s]
04:24:31  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
04:24:31  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.04s]
04:24:31  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
04:24:31  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
04:24:31  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.07s]
04:24:31  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
04:24:31  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.13s]
04:24:31  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
04:24:31  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.06s]
04:24:31  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
04:24:31  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.08s]
04:24:31  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
04:24:31  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.10s]
04:24:31  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
04:24:31  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.06s]
04:24:31  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
04:24:31  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.07s]
04:24:31  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.64s]
04:24:31  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
04:24:31  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.06s]
04:24:31  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
04:24:32  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.04s]
04:24:32  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
04:24:32  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.05s]
04:24:32  
04:24:32  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 9.01 seconds (9.01s).
04:24:32  
04:24:32  Completed successfully
04:24:32  
04:24:32  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
```

#### 2. Output parity

Lệnh chạy từ thư mục gốc repo: `.\.venv\Scripts\python.exe -m scripts.parity`.

```text
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

dbt build thành công với 19 mục PASS, không có cảnh báo hay lỗi. Parity xác nhận hai bảng chung của pipeline Python và dbt có checksum khớp nhau; phép so sánh này không bao gồm các bảng khác của pipeline Python.