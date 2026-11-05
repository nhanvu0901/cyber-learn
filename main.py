"""Kiểm thử tải HTTP có giới hạn; chỉnh CONFIG rồi chạy python3 main.py."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import http.client
import math
import time
from urllib.parse import urlsplit


# ================= CẤU HÌNH — CHỈNH TẠI ĐÂY =================
# Sau khi điền URL, chạy: python3 main.py
# Tham số dòng lệnh được ưu tiên hơn giá trị ở đây, ví dụ:
# python3 main.py http://localhost:8000 --rates 1 5 --stage-seconds 30
CONFIG = {
    # URL đầy đủ của endpoint thuộc phạm vi kiểm thử, gồm http:// hoặc https://.
    # Ví dụ: 'http://localhost:8000/health'. Để trống sẽ báo lỗi, không gửi request.
    "url": "test",

    # Số request/giây MỤC TIÊU cho từng mức, chạy lần lượt từ trái sang phải.
    # (1, 5, 10, 20) nghĩa là 4 mức tải; đây không phải số người dùng đồng thời.
    "rates": (1, 5, 10, 20),

    # Thời gian phát request ở MỖI mức, đơn vị giây.
    # 4 mức x 60 = khoảng 240 giây, cộng thời gian chờ request cuối hoàn thành.
    "stage_seconds": 60,

    # Tối đa bao nhiêu request được xử lý đồng thời trên máy chạy script.
    # Khi đủ số này, lượt gửi mới bị bỏ qua, không xếp hàng vô hạn.
    # Tăng giá trị này không tự tăng request/giây; tốc độ do rates quyết định.
    "max_workers": 10,

    # Timeout cho thao tác mạng và ngân sách đọc nội dung phản hồi, đơn vị giây.
    # Request bị timeout được tính là lỗi. Đây không phải thời lượng mỗi mức
    # hay giới hạn tổng thời gian tuyệt đối cho cả request (gồm DNS/kết nối/đọc).
    "timeout_seconds": 5,

    # Ngưỡng độ trễ p95, đơn vị giây: 95% request phải hoàn thành trong ngưỡng này.
    # Đo đến khi đọc xong nội dung phản hồi, không phải thời gian dựng trang web.
    # Nếu p95 vượt ngưỡng, script dừng tăng tải SAU KHI mức hiện tại kết thúc.
    "max_p95_seconds": 1,

    # Tỷ lệ lỗi cho phép, từ 0 đến 1: 0.01 = 1%, 0.05 = 5%, 0 = không cho lỗi.
    # Lỗi gồm HTTP ngoài 200–299, lỗi mạng, timeout, phản hồi thiếu hoặc > 8 MiB.
    # Vượt ngưỡng này cũng dừng tăng tải sau khi mức hiện tại kết thúc.
    "max_error_rate": 0.01,
}
# Nếu có lượt gửi bị bỏ qua, script cũng dừng sau mức đó vì chưa phát đủ tải.
# ============================================================


def _request(parsed, timeout_seconds):
    connection_type = (http.client.HTTPSConnection if parsed.scheme == 'https'
                       else http.client.HTTPConnection)
    connection = connection_type(parsed.hostname, parsed.port,
                                 timeout=timeout_seconds)
    path = parsed.path or '/'
    if parsed.query:
        path += '?' + parsed.query
    start = time.perf_counter()
    status = None
    error = None
    try:
        connection.request('GET', path, headers={'User-Agent': 'BoundedLoadTest/1.0'})
        response = connection.getresponse()
        status = response.status
        # Stream the complete body, bounded by time and size.
        size = 0
        while True:
            if time.perf_counter() - start > timeout_seconds:
                raise TimeoutError('Vượt thời gian tải phản hồi')
            chunk = response.read1(65536)
            if not chunk:
                if response.length:
                    raise http.client.IncompleteRead(b'', response.length)
                break
            size += len(chunk)
            if size > 8 * 1024 * 1024:
                raise ValueError('Phản hồi vượt giới hạn 8 MiB')
        if not 200 <= status < 300:
            error = f'HTTP {status}'
    except (OSError, http.client.HTTPException, ValueError) as exc:
        error = str(exc)
    finally:
        connection.close()
    return status, time.perf_counter() - start, error


def run_load_test(
    website_url,
    *,
    rates=CONFIG['rates'],
    stage_seconds=CONFIG['stage_seconds'],
    max_workers=CONFIG['max_workers'],
    timeout_seconds=CONFIG['timeout_seconds'],
    max_p95_seconds=CONFIG['max_p95_seconds'],
    max_error_rate=CONFIG['max_error_rate'],
):
    """Run paced stages; stop after a failed or under-delivered stage.

    Requests use fresh connections, with no redirects/retries. Non-2xx statuses,
    transport errors and bodies above 8 MiB count as failures. Stage duration is
    the submission window; pending requests drain afterwards. p95 includes failed
    requests and measures full response latency, not browser rendering time.
    """
    parsed = urlsplit(website_url)
    if (parsed.scheme not in ('http', 'https') or not parsed.hostname
            or parsed.username is not None or parsed.password is not None):
        raise ValueError('Cần URL http:// hoặc https:// hợp lệ, không chứa mật khẩu')
    _ = parsed.port  # Validate before starting workers.
    rates = tuple(rates)
    positive_values = (*rates, stage_seconds, timeout_seconds, max_p95_seconds)
    if not rates or any(not math.isfinite(v) or v <= 0 for v in positive_values):
        raise ValueError('Tốc độ, thời lượng, timeout và ngưỡng p95 phải dương, hữu hạn')
    if not isinstance(max_workers, int) or max_workers < 1:
        raise ValueError('Số kết nối đồng thời phải là số nguyên dương')
    if not math.isfinite(max_error_rate) or not 0 <= max_error_rate <= 1:
        raise ValueError('Tỷ lệ lỗi cho phép phải nằm trong khoảng 0–1')

    print(f'Đích: {website_url} | Tối đa {max_workers} kết nối đồng thời', flush=True)
    results = []
    for rate in rates:
        print(f'\nBắt đầu mức {rate:g} req/s trong {stage_seconds:g}s…', flush=True)
        futures = []
        pending = set()
        skipped = 0
        slot = 0
        start = time.perf_counter()
        deadline = start + stage_seconds
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                now = time.perf_counter()
                scheduled = start + slot / rate
                if now >= deadline:
                    skipped += max(0, math.ceil(rate * stage_seconds) - slot)
                    break
                if scheduled >= deadline:
                    time.sleep(min(deadline - now, 0.1))
                    continue
                if now < scheduled:
                    time.sleep(min(scheduled - now, 0.1))
                    continue
                # Skip missed slots instead of catching up with bursts.
                current_slot = max(slot, math.floor((now - start) * rate))
                skipped += current_slot - slot
                slot = current_slot + 1
                pending = {future for future in pending if not future.done()}
                if len(pending) >= max_workers:
                    skipped += 1
                    continue
                future = executor.submit(_request, parsed, timeout_seconds)
                pending.add(future)
                futures.append(future)
            # At most max_workers requests drain; no growing submission queue.
        elapsed = time.perf_counter() - start
        outcomes = [future.result() for future in futures]
        latencies = sorted(outcome[1] for outcome in outcomes)
        count = len(outcomes)
        errors = sum(error is not None for _, _, error in outcomes)
        p95 = latencies[math.ceil(0.95 * count) - 1] if count else 0
        error_rate = errors / count if count else 1
        statuses = dict(Counter(status for status, _, _ in outcomes if status is not None))
        passed = (count > 0 and p95 <= max_p95_seconds
                  and error_rate <= max_error_rate and skipped == 0)
        result = {
            'target_rps': rate, 'actual_rps': count / elapsed,
            'requests': count, 'errors': errors, 'error_rate': error_rate,
            'p95_seconds': p95, 'statuses': statuses, 'skipped': skipped,
            'elapsed_seconds': elapsed, 'passed': passed,
        }
        results.append(result)
        print(f'Thực tế: {result["actual_rps"]:.2f} req/s | {count} request | '
              f'p95: {p95 * 1000:.1f} ms | Lỗi: {errors} ({error_rate:.1%}) | '
              f'Bỏ qua: {skipped} | HTTP: {statuses}', flush=True)
        error_counts = Counter(error for _, _, error in outcomes if error is not None)
        for message, occurrences in error_counts.most_common(3):
            print(f'  {occurrences} lỗi: {message}')
        if not passed:
            print('Dừng tăng tải: vượt ngưỡng p95/tỷ lệ lỗi, hoặc không phát đủ tải.',
                  flush=True)
            break
        print('Đạt ngưỡng ở mức này.', flush=True)
    return results


def main():
    parser = argparse.ArgumentParser(
        description='Kiểm thử tải HTTP; mặc định lấy từ CONFIG ở đầu main.py.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('url', nargs='?', default=CONFIG['url'],
                        help='URL HTTP(S); có thể điền sẵn trong CONFIG')
    parser.add_argument('--rates', type=float, nargs='+', default=CONFIG['rates'],
                        help='Request/giây ở mỗi mức')
    parser.add_argument('--stage-seconds', type=float, default=CONFIG['stage_seconds'],
                        help='Thời gian phát tải mỗi mức; chờ request đang chạy sau đó')
    parser.add_argument('--max-workers', type=int, default=CONFIG['max_workers'],
                        help='Số kết nối đồng thời tối đa')
    parser.add_argument('--timeout-seconds', type=float, default=CONFIG['timeout_seconds'],
                        help='Timeout I/O và ngân sách đọc body, đơn vị giây')
    parser.add_argument('--max-p95-seconds', type=float, default=CONFIG['max_p95_seconds'],
                        help='Ngưỡng p95, đơn vị giây')
    parser.add_argument('--max-error-rate', type=float, default=CONFIG['max_error_rate'],
                        help='Tỷ lệ lỗi cho phép, từ 0 đến 1')
    args = vars(parser.parse_args())
    url = args.pop('url')
    try:
        results = run_load_test(url, **args)
    except ValueError as exc:
        parser.error(str(exc))
    except KeyboardInterrupt:
        print('\nĐã dừng theo yêu cầu.')
        return 130
    return 0 if all(stage['passed'] for stage in results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
