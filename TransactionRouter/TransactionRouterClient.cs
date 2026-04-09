using System.Buffers;
using System.Collections.Concurrent;
using System.Threading.Channels;
using Google.Protobuf;
using NATS.Client.Core;
using TransactionRouter.Proto;
using Gcoder.Poll; // Không gian tên của lớp ObjectPool mới

namespace TransactionRouter;

public class TransactionRouterClient : IAsyncDisposable
{
    // Struct lưu thông tin message cần gửi (Struct giúp tránh GC so với Class)
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

    // TỐI ƯU: Thread-safe, thay thế List + lock, không cần .ToArray() gây rác
    private readonly ConcurrentDictionary<CancellationTokenSource, byte> _ctsList = new();

    // Channel để đẩy việc gửi message sang một luồng ngầm (tránh block luồng game)
    private readonly Channel<SendPayload> _sendChannel =
        Channel.CreateUnbounded<SendPayload>(new UnboundedChannelOptions { SingleReader = true });

    private readonly Dictionary<string, List<SendPayload>> _pendingPubs = [];

    // TỐI ƯU: Tái sử dụng request để ghép lô (batch) thay vì tạo mới
    private readonly BatchTransactionRequest _batchRequest = new();
    private readonly List<byte[]> _currentBatchBuffers = [];

    public TransactionRouterClient(string[] natsUrl, int id)
    {
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

                    // Xóa danh sách batch của vòng lặp trước (Zero GC)
                    foreach (var list in _pendingPubs.Values)
                    {
                        list.Clear();
                    }

                    // Đọc hết các message đang chờ trong Channel để gom lô
                    while (reader.TryRead(out var payload))
                    {
                        if (!_pendingPubs.TryGetValue(payload.Subject, out var list))
                        {
                            list = []; // Khởi tạo 1 lần duy nhất cho mỗi chủ đề mới
                            _pendingPubs[payload.Subject] = list;
                        }

                        list.Add(payload);
                    }

                    await FlushPendingAsync();
                }
                catch (OperationCanceledException)
                {
                    break; // Dừng tiến trình khi có lệnh hủy
                }
                catch (Exception)
                {
                    // Lỗi rớt mạng hoặc bất ngờ, có thể thêm logging ở đây
                }
            }
        }
        finally
        {
            // Dọn dẹp rác khi thoát vòng lặp, trả lại toàn bộ buffer đang kẹt
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

        // Mượn mảng từ Pool hệ thống
        byte[] buffer = ArrayPool<byte>.Shared.Rent(size);
        message.WriteTo(buffer.AsSpan(0, size));

        // Nếu Channel bị đóng (server đang tắt), trả lại buffer
        if (!_sendChannel.Writer.TryWrite(new SendPayload(subject, buffer, size, accountId)))
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// Đăng ký nhận tin nhắn. 
    /// LƯU Ý: Yêu cầu hàm resetAction để dọn dẹp các trường RepeatedField/List của Protobuf khi trả về Pool.
    /// </summary>
    public IDisposable Subscribe<T>(string subject, Action<T, long> handler, Action<T>? resetAction = null)
        where T : class, IMessage, new()
    {
        string finalSubject = $"{subject}.{_id}";
        var cts = new CancellationTokenSource();
        var ct = cts.Token;

        // Lưu vào dictionary an toàn đa luồng
        _ctsList.TryAdd(cts, 0);

        _ = SubscribeAsync(finalSubject, ct, handler, resetAction);

        return new SubscriptionHandle(cts, RemoveCts);
    }

    private async Task SubscribeAsync<T>(string finalSubject, CancellationToken ct, Action<T, long> handler,
        Action<T>? resetAction = null)
        where T : class, IMessage, new()
    {
        // TỐI ƯU: Sử dụng ObjectPool để không khởi tạo 'new T()' liên tục
        var pool = new ObjectPool<T>(resetAction ?? (_ => { }));

        // TỐI ƯU: Tái sử dụng một đối tượng duy nhất để Deserialize
        var reusableBatchRequest = new BatchTransactionRequest();

        while (!ct.IsCancellationRequested)
        {
            try
            {
                // ZERO GC: Dùng NatsMemoryOwner<byte> thay vì byte[] để mượn vùng nhớ trực tiếp từ NATS
                await foreach (var msg in _nats.SubscribeAsync<NatsMemoryOwner<byte>>(finalSubject,
                                   cancellationToken: ct))
                {
                    // Kiểm tra tin nhắn trống (struct không thể null)
                    if (msg.Data.Length == 0) continue;

                    try
                    {
                        // Xoá và Merge (Zero Allocation) thay vì ParseFrom
                        reusableBatchRequest.Payload.Clear();
                        reusableBatchRequest.PlayerIds.Clear();
                        reusableBatchRequest.RouterId = 0;

                        // Truy cập trực tiếp vào Span của NATS, KHÔNG copy dữ liệu
                        reusableBatchRequest.MergeFrom(msg.Data.Memory.Span);

                        for (var i = 0; i < reusableBatchRequest.PlayerIds.Count; i++)
                        {
                            var slice = reusableBatchRequest.Payload[i];
                            var playerId = reusableBatchRequest.PlayerIds[i];

                            // Mượn Event từ Pool
                            var evt = pool.Rent();
                            try
                            {
                                evt.MergeFrom(slice.Span);
                                handler(evt, playerId); // Xử lý logic
                            }
                            finally
                            {
                                // Luôn luôn trả về Pool, dù Handler có xảy ra lỗi
                                pool.Return(evt);
                            }
                        }
                    }
                    catch (Exception)
                    {
                        /* Bỏ qua lỗi Deserialize */
                    }
                    finally
                    {
                        // BẮT BUỘC: Trả vùng nhớ (buffer) lại cho NATS internal pool
                        // Nếu thiếu dòng này, RAM sẽ bị rò rỉ rất nhanh.
                        msg.Data.Dispose();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception)
            {
                // Mất kết nối NATS, chờ 2s rồi thử lại
                try
                {
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

                // Đóng gói mảng byte không cần sao chép
                _batchRequest.Payload.Add(UnsafeByteOperations.UnsafeWrap(memorySegment));
                _batchRequest.PlayerIds.Add(payload.PlayerId);

                // Ghi nhớ để tí nữa trả lại Pool
                _currentBatchBuffers.Add(payload.Buffer);

                // Nếu lô hàng quá dung lượng, tiến hành gửi ngay
                if (_batchRequest.CalculateSize() > MaxBatchPayloadSize)
                {
                    await SendCurrentBatch(subject);
                }
            }

            // Gửi phần còn dư
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
            // Có thể thêm logging lỗi gửi ở đây
        }
        finally
        {
            // Trả buffer tổng
            ArrayPool<byte>.Shared.Return(buffer);

            // Trả buffer các phần tử con
            foreach (var b in _currentBatchBuffers)
            {
                ArrayPool<byte>.Shared.Return(b);
            }

            // Reset trạng thái lô hàng
            _batchRequest.Payload.Clear();
            _batchRequest.PlayerIds.Clear();
            _currentBatchBuffers.Clear();
        }
    }

    private void RemoveCts(CancellationTokenSource cts)
    {
        _ctsList.TryRemove(cts, out _);
    }

    public async ValueTask DisposeAsync()
    {
        _sendChannel.Writer.TryComplete();

        if (_cts != null)
        {
            _cts.Cancel();
        }

        if (_sendLoopTask != null)
        {
            try
            {
                await _sendLoopTask;
            }
            catch
            {
                /* Ignore */
            }
        }

        _cts?.Dispose();

        // TỐI ƯU: Đóng an toàn các Subscriptions mà không tạo Array
        foreach (var cts in _ctsList.Keys)
        {
            try
            {
                cts.Cancel();
            }
            catch
            {
                /* Ignore */
            }

            cts.Dispose();
        }

        _ctsList.Clear();

        await _nats.DisposeAsync();
    }

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

            try
            {
                _cts.Cancel();
            }
            catch
            {
                /* Ignore */
            }

            _onDispose(_cts);
            _cts.Dispose();
        }
    }
}