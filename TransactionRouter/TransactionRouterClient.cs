using System.Buffers;
using System.Threading.Channels;
using Google.Protobuf;
using NATS.Client.Core;
using TransactionRouter.Proto;

namespace TransactionRouter;

public class TransactionRouterClient : IAsyncDisposable
{
    private readonly struct SendPayload(string subject, byte[] buffer, int length, long playerId)
    {
        public readonly string Subject = subject;
        public readonly byte[] Buffer = buffer;
        public readonly int Length = length;
        public readonly long PlayerId = playerId;
    }


    private readonly NatsConnection _nats;
    private readonly int _id;
    private Task? _sendLoopTask;
    private CancellationTokenSource? _cts;
    private const int MaxBatchPayloadSize = 50 * 1024; // 50KB

    private List<CancellationTokenSource> _ctsList = [];
    private object _ctsListLock = new();

    private readonly Channel<SendPayload> _sendChannel =
        Channel.CreateUnbounded<SendPayload>(new UnboundedChannelOptions { SingleReader = true });

    private readonly Dictionary<string, List<SendPayload>> _pendingPubs = [];
    private readonly BatchTransactionRequest _batchRequest = new();
    private readonly List<byte[]> _currentBatchBuffers = [];

    public TransactionRouterClient(string[] natsUrl, int id)
    {
        // join natsUrl with ',"
        string url = string.Join(',', natsUrl);
        var opts = NatsOpts.Default with
        {
            Url = url,
            Name = $"TransactionRouterClient-{id}"
        };
        _nats = new NatsConnection(opts);
        _id = id;
        _batchRequest.RouterId = id;
    }

    public async ValueTask ConnectAsync()
    {
        await _nats.ConnectAsync();
        StartSendLoop();
    }

    private void StartSendLoop()
    {
        _cts = new CancellationTokenSource();
        _sendLoopTask = Task.Factory.StartNew(
            () => SendLoopAsync(_cts.Token),
            _cts.Token,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default
        ).Unwrap();
    }

    private async Task SendLoopAsync(CancellationToken ct)
    {
        var reader = _sendChannel.Reader;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await reader.WaitToReadAsync(ct);

                    foreach (var list in _pendingPubs.Values)
                    {
                        list.Clear();
                    }

                    while (reader.TryRead(out var payload))
                    {
                        if (!_pendingPubs.TryGetValue(payload.Subject, out var list))
                        {
                            list = [];
                            _pendingPubs[payload.Subject] = list;
                        }

                        list.Add(payload);
                    }

                    await FlushPendingAsync();
                }
                catch (OperationCanceledException)
                {
                    // Lệnh tắt từ CancellationToken -> chủ động thoát vòng lặp
                    break;
                }
                catch (Exception)
                {
                    // FIX VẤN ĐỀ 2: Bắt lỗi (VD: đứt mạng NATS) để vòng lặp không bị crash chết ngầm.
                    // Bạn nên gọi _logger.LogError(ex, "Lỗi khi flush batch") ở đây nếu có ILogger.
                }
            }
        }
        finally
        {
            // FIX VẤN ĐỀ 3: Dọn dẹp rác khi thoát vòng lặp (Dispose)
            // Lấy hết các message còn kẹt lại trong Channel và trả buffer về Pool
            while (reader.TryRead(out var payload))
            {
                ArrayPool<byte>.Shared.Return(payload.Buffer);
            }
        }
    }

    public void Publish(long accountId, string subject, IMessage message)
    {
        int size = message.CalculateSize();
        if (size == 0) return;

        byte[] buffer = ArrayPool<byte>.Shared.Rent(size);
        message.WriteTo(buffer.AsSpan(0, size));

        // NẾU: Channel đã đóng (server đang tắt), TryWrite = false -> Trả buffer về Pool
        if (!_sendChannel.Writer.TryWrite(new SendPayload(subject, buffer, size, accountId)))
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public IDisposable Subscribe<T>(string subject, Action<T, long> handler)
        where T : IMessage, new()
    {
        string finalSubject = $"{subject}.{_id}";
        var cts = new CancellationTokenSource();
        var ct = cts.Token;

        lock (_ctsListLock)
        {
            _ctsList.Add(cts);
        }

        _ = SubscribeAsync(finalSubject, ct, handler);

        // Trả về Handle. Truyền kèm theo hàm RemoveCts để Handle có thể tự gỡ nó ra khỏi list
        return new SubscriptionHandle(cts, RemoveCts);
    }

    private async Task SubscribeAsync<T>(string finalSubject, CancellationToken ct, Action<T, long> handler)
        where T : IMessage, new()
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await foreach (var msg in _nats.SubscribeAsync<byte[]>(finalSubject, cancellationToken: ct))
                {
                    if (msg.Data is null) continue;
                    try
                    {
                        BatchTransactionRequest batchRequest = BatchTransactionRequest.Parser.ParseFrom(msg.Data);
                        for (var i  = 0 ; i < batchRequest.PlayerIds.Count; i ++)
                        {
                            var slice = batchRequest.Payload[i];
                            var playerId = batchRequest.PlayerIds[i];
                            var evt = new T();
                            evt.MergeFrom(slice.Span);
                            handler(evt, playerId);
                        }
                    }
                    catch (Exception)
                    {
                        /* Parse/handler error — bỏ qua */
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Nhận lệnh tắt server từ hàm Dispose -> Thoát hẳn
                break;
            }
            catch (Exception ex)
            {
                // Rớt mạng hoặc NATS crash. 
                // _logger.LogWarning(ex, "Mất kết nối NATS ở Subject Watcher. Đang thử lại...");

                try
                {
                    // Nghỉ 2 giây để tránh dội bom CPU rồi vòng lên try lại await foreach
                    await Task.Delay(2000, ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }


    private async ValueTask FlushPendingAsync()
    {
        foreach (var entry in _pendingPubs)
        {
            string subject = entry.Key;
            var payloadList = entry.Value;
            if (payloadList.Count == 0) continue;

            _batchRequest.Payload.Clear();
            _batchRequest.PlayerIds.Clear();
            _currentBatchBuffers.Clear();

            foreach (var payload in payloadList)
            {
                var memorySegment = payload.Buffer.AsMemory(0, payload.Length);
                _batchRequest.Payload.Add(UnsafeByteOperations.UnsafeWrap(memorySegment));
                _batchRequest.PlayerIds.Add(payload.PlayerId);

                // Track lại để tí nữa trả về Pool
                _currentBatchBuffers.Add(payload.Buffer);

                // Kiểm tra kích thước sau khi add
                if (_batchRequest.CalculateSize() > MaxBatchPayloadSize)
                {
                    await SendCurrentBatch(subject);
                }
            }

            // Gửi phần còn lại nếu có
            if (_batchRequest.PlayerIds.Count > 0)
            {
                await SendCurrentBatch(subject);
            }
        }
    }

    private async Task SendCurrentBatch(string subject)
    {
        int size = _batchRequest.CalculateSize();
        byte[] buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            _batchRequest.WriteTo(buffer.AsSpan(0, size));
            await _nats.PublishAsync(subject, buffer.AsMemory(0, size));
        }
        catch (Exception)
        {
            // Log error
        }
        finally
        {
            // Trả buffer tổng của batch
            ArrayPool<byte>.Shared.Return(buffer);

            // Trả CÁC buffer con của các message nhỏ về Pool một cách an toàn
            foreach (var b in _currentBatchBuffers)
            {
                ArrayPool<byte>.Shared.Return(b);
            }

            // Reset state
            _batchRequest.Payload.Clear();
            _batchRequest.PlayerIds.Clear();
            _currentBatchBuffers.Clear();
        }
    }

    private void RemoveCts(CancellationTokenSource cts)
    {
        lock (_ctsListLock)
        {
            _ctsList.Remove(cts);
        }
    }


    public async ValueTask DisposeAsync()
    {
        // 1. Dừng nhận message mới vào channel (tuỳ chọn nhưng khuyên dùng)
        _sendChannel.Writer.TryComplete();

        // 2. Kích hoạt Cancel để SendLoopAsync thoát khỏi WaitToReadAsync
        if (_cts != null)
        {
            _cts.Cancel();
        }

        // 3. Đợi Loop kết thúc toàn bộ công việc đang làm dở
        if (_sendLoopTask != null)
        {
            try
            {
                await _sendLoopTask;
            }
            catch
            {
                /* Ignore task cancellation exception */
            }
        }

        // 4. Dispose CancellationTokenSource
        if (_cts != null) _cts.Dispose();

        // 5. Huỷ các Token của Subscriber
        CancellationTokenSource[] snapShot;
        lock (_ctsListLock)
        {
            snapShot = _ctsList.ToArray();
            _ctsList.Clear(); // Tránh rò rỉ nếu Dispose được gọi nhiều lần
        }

        foreach (var cts in snapShot)
        {
            try
            {
                cts.Cancel();
            }
            catch
            {
                // Ignore
            }

            cts.Dispose();
        }

        // 6. CUỐI CÙNG mới tắt NATS connection để đảm bảo các tiến trình gửi cuối được hoàn tất
        await _nats.DisposeAsync();
    }

    // Đặt bên trong class TransactionRouterClient
    private sealed class SubscriptionHandle : IDisposable
    {
        private readonly CancellationTokenSource _cts;
        private readonly Action<CancellationTokenSource> _onDispose;
        private bool _disposed;

        public SubscriptionHandle(CancellationTokenSource cts, Action<CancellationTokenSource> onDispose)
        {
            _cts = cts;
            _onDispose = onDispose;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            // 1. Dừng vòng lặp nhận message
            try
            {
                _cts.Cancel();
            }
            catch
            {
            }

            // 2. Báo cho class cha xóa token này khỏi list
            _onDispose(_cts);

            // 3. Giải phóng bộ nhớ của token
            _cts.Dispose();
        }
    }
}