# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Đào Nhật Anh  **Lớp:** E403  **Mã học viên:** 2A202601464  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 25.6s
  run 2/3 … 26.3s
  run 3/3 … 29.7s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  bài mở rộng (EXTRA.md)                      — chưa chạy `make seed-extra`
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT   4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| Item | Details |
|---|---|
| **Triệu chứng** | Khi Airflow runner bị khôi phục hoặc người trực bấm Clear Task chạy lại job, kích thước bảng `gold_training_set` liên tục phình to (tăng hàng chục nghìn hàng), không phát sinh thông báo lỗi. |
| **Nguyên nhân** | Model `gold_training_set` được khai báo `materialized = 'incremental'` nhưng **thiếu** `unique_key` và `incremental_strategy`. Trong dbt, khi thiếu `unique_key`, chiến lược ghi mặc định là `append` (`INSERT INTO`). Khi chạy lại cùng partition ngày, các bản ghi cũ không được thay thế mà bị chèn thêm. Đồng thời, dữ liệu CDC chứa các thao tác cập nhật (`op = 'u'`), dẫn đến một ticket được cập nhật nhiều lần sẽ đi qua điều kiện lọc theo `run_date` ở nhiều partition ngày khác nhau. Ngoài ra, DAG Airflow ban đầu bật `catchup = True` và chưa giới hạn `max_active_runs`, dẫn đến nhiều lần chạy bị dồn và ghi đồng thời vào cùng một bảng. |
| **Cách khắc phục** | 1. Sửa `dbt/models/gold/gold_training_set.sql`: Cấu hình `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`.<br>2. Sửa `dags/ai_training_pipeline.py`: Đặt `catchup = False` và `max_active_runs = 1`. |
| **Bằng chứng** | trước: 38,750 hàng (thừa 26,270 hàng) · sau: 12,480 hàng (đúng 1 hàng / 1 ticket) · checksum 3 lượt: `8dd7c98653` (giống hệt nhau) |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| Item | Details |
|---|---|
| **Triệu chứng** | `gold_feature_daily` báo ỔN ĐỊNH ✓ nhưng hụt 455 hàng so với đối chiếu (chỉ có 8,645 / 9,100 hàng). Các ngày mới đủ hàng nhưng các ngày quá khứ bị thiếu. |
| **P99 độ trễ đo được** | **2.73 ngày** (P50 = 0.13 ngày, P95 = 1.81 ngày, P99 = 2.73 ngày, Max = 2.94 ngày; 5.05% bản ghi có độ trễ trễ hơn 1 ngày) |
| **Lookback đã chọn** | **3 ngày** — vì P99 độ trễ đo được là 2.73 ngày (<= 3 ngày). Khoảng thời gian này đảm bảo phủ được trên 99% lượng dữ liệu tới muộn mà không tiêu tốn tài nguyên vô ích cho việc tính toán lại các ngày quá xa. |
| **Nguyên nhân** | Điều kiện lọc incremental ban đầu `where event_date > (select max(event_date) from {{ this }})` chỉ xử lý các sự kiện có `event_date` lớn hơn hẳn ngày lớn nhất đã có trong bảng đích. Nếu một sự kiện xảy ra ngày 08-12 nhưng đến kho muộn vào ngày 08-15, `max(event_date)` trong target đã đạt 08-14, khiến sự kiện ngày 08-12 bị loại bỏ vĩnh viễn ở mọi lượt chạy sau. |
| **Cách khắc phục** | 1. Sửa `dbt/models/gold/gold_feature_daily.sql`: Nới cửa sổ tính toán lại bằng `where event_date >= (select max(event_date) - interval 3 day from {{ this }})`.<br>2. Cấu hình `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'delete+insert'` để ghi đè (thay thế) các cặp `(event_date, customer_id)` trong cửa sổ 3 ngày tính lại, ngăn ngừa cộng dồn dữ liệu lặp. |
| **Bằng chứng** | trước: 8,645 hàng · sau: 9,100 hàng (đủ 14 ngày × 650 customer) · checksum 3 lượt: `3db448685c` |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Chọn **P99** (2.73 ngày ~ 3 ngày) làm căn cứ giúp cân bằng giữa tính đầy đủ của dữ liệu và chi phí vận hành. P99 đảm bảo 99% dữ liệu tới muộn được xử lý đúng với chi phí cố định (chỉ tính lại 3 ngày gần nhất ở mỗi lượt chạy). Nếu chọn `max` (hoặc mở rộng vô hạn), mỗi ngày có một bản ghi cực đoan trễ 14-30 ngày sẽ bắt đường ống phải tính lại toàn bộ 30 ngày quá khứ ở MỌI lượt chạy sau, làm thời gian thực thi tăng vọt, tiêu tốn I/O, CPU và bộ nhớ không cần thiết.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| Item | Details |
|---|---|
| **Triệu chứng** | Team backend thay đổi kiểu cột `priority` từ số sang chuỗi từ ngày 08-10. Pipeline không phát sinh ngoại lệ dừng, nhưng model dự đoán kém; `silver_tickets.priority` chứa 6,606 bản ghi sai (chứa NULL và các số ngoài khoảng 1..4 như 0, 5, -1). |
| **Nguyên nhân** | Macro `normalize_priority` cũ dùng `try_cast(priority_raw as integer)`, làm toàn bộ nhãn chuỗi hợp lệ (`urgent`, `high`, `medium`, `low`) bị chuyển thành `NULL` (loại bỏ nhầm dữ liệu tốt), đồng thời cho phép các số không hợp lệ (`0`, `5`, `-1`) lọt qua. Ngoài ra, trong `silver_tickets.sql`, việc xếp hạng `row_number()` diễn ra TRƯỚC khi lọc bản ghi lỗi, khiến ticket nào có bản ghi CDC mới nhất bị hỏng thì toàn bộ ticket đó bị biến mất khỏi Silver. Contract cũng chưa được bật (`enforced: false`). |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **Nhóm 1** (`'1'`, `'2'`, `'3'`, `'4'`): Đúng contract ban đầu -> Giữ nguyên (chuyển về int).<br>**Nhóm 2** (`'urgent'`, `'high'`, `'medium'`, `'low'`): Schema evolution (thay đổi cách ghi, giữ nguyên ý nghĩa) -> Map tương ứng: `urgent->1`, `high->2`, `medium->3`, `low->4`.<br>**Nhóm 3** (`'P1'`, `'unknown'`, `'0'`, `'5'`, `'-1'`, `''`, `NULL`): Dữ liệu hỏng thật -> Trả về `NULL` để định tuyến vào `quarantine_tickets`. |
| **Cách khắc phục** | 1. Sửa `dbt/macros/normalize_priority.sql`: Viết lại macro dùng `CASE` cho 3 nhóm giá trị và định nghĩa `priority_reject_reason`.<br>2. Sửa `dbt/models/silver/silver_tickets.sql`: Lọc các bản ghi hợp lệ (`where priority_clean is not null`) TRƯỚC khi xếp hạng `row_number()`.<br>3. Sửa `dbt/models/silver/quarantine_tickets.sql`: Thêm điều kiện `where normalize_priority(priority_raw) is null`.<br>4. Sửa `dbt/models/silver/schema.yml`: Đặt `enforced: true` cho contract và bật các test `not_null` + `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `silver_tickets` = 12,480 ticket · `silver_tickets.priority` sạch 100% (không NULL, ∈ 1..4) · `dbt test` 11/11 pass |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> **1. Nên chặn ở tầng Silver.** Tầng Bronze đóng vai trò là "Data Lake / Landing zone" giữ nguyên bản gốc dữ liệu thô (raw data). Nếu từ chối hoặc chặn bản ghi lỗi ngay tại Bronze, dữ liệu đó sẽ bị mất vĩnh viễn, vô hiệu hoá khả năng truy vết (audit trail) và chẩn đoán nguyên nhân sự cố của đội kỹ thuật.
> **2. Không nên để pipeline dừng khi gặp bản ghi lỗi.** Trong vận hành thực tế, 312 bản ghi lỗi chiếm tỷ lệ rất nhỏ (< 0.25%) trên tổng số hơn 130,000 sự kiện. Nếu cho pipeline dừng (crash/fail DAG), toàn bộ dữ liệu hợp lệ còn lại sẽ bị tắc nghẽn, ảnh hưởng đến các ứng dụng downstream (RAG, routing, dashboard). Giải pháp chuẩn Data Engineering là **Quarantine Pattern** — tách riêng bản ghi hỏng vào bảng cách ly để đội vận hành xử lý sau, đồng thời cho phép luồng chính tiếp tục chạy thông suốt.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| Item | Details |
|---|---|
| **Bài đã làm** | Không làm (tùy chọn) |
| **Nguyên nhân** | — |
| **Cách khắc phục** | — |
| **Bằng chứng** | — |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra toàn bộ các incremental model của dbt xem đã khai báo đầy đủ `unique_key` và `incremental_strategy` chưa; đồng thời kiểm tra tham số DAG (`catchup`, `max_active_runs`) trên Airflow để tránh hiện tượng ghi trùng lặp khi rerun. |
| 2 | Phân tích phân bố độ trễ của dữ liệu nguồn (`ingestion_delay` percentile: P50, P95, P99) thay vì giả định dữ liệu luôn đến đúng giờ. Thiết lập lookback window dựa trên P99 đi kèm `unique_key` thích hợp. |
| 3 | Kiểm tra sự hiện diện của Data Contract (`enforced: true`) và các dbt test ràng buộc miền giá trị ở tầng Silver; đảm bảo lọc bản ghi lỗi trước khi deduplicate/rank và thiết lập bảng Quarantine để cách ly lỗi an toàn. |
