using System.Buffers;
using System.Collections.Concurrent;
using System.Threading.Channels;
using Google.Protobuf;
using Microsoft.Extensions.ObjectPool; // Thư viện mới
using NATS.Client.Core;
using TransactionRouter.Proto;

namespace TransactionRouter;

public class TransactionRouterClient : IAsyncDisposable
{
    // Chính sách tái sử dụng object cho Microsoft Pool
    private sealed class ProtobufPoolPolicy<T> : IPooledObjectPolicy<T> where T : class, IMessage, new()
    {
        private readonly Action<T>? _resetAction;
        public ProtobufPoolPolicy(Action<T>? resetAction) => _resetAction = resetAction;
        public T Create() => new T();
        public bool Return(T obj)
        {
            _resetAction?.Invoke(obj);
            return true;
        }
    }

    private readonly struct SendPayload(string subject, byte[] buffer, int length, long playerId)
    {
        public readonly string Subject = subject;
        public readonly byte[] buffer = buffer;
        public readonly int Length = length;
        public readonly long PlayerId = playerId;
    }

    private readonly NatsConnection _nats;
    private readonly int _id;
    private Task? _sendLoopTask;
    private CancellationTokenSource? _cts;
    private const int MaxBatchPayloadSize = 50 * 1024;

    private readonly ConcurrentDictionary<CancellationTokenSource, byte> _ctsList = new();
    private readonly DefaultObjectPoolProvider _poolProvider = new(); // Quản lý các Pool

    private readonly Channel<SendPayload> _sendChannel =
        Channel.CreateUnbounded<SendPayload>(new UnboundedChannelOptions { SingleReader = true });

    private readonly Dictionary<string, List<SendPayload>> _pendingPubs = [];
    private readonly BatchTransactionRequest _batchRequest = new();
    private readonly List<byte[]> _currentBatchBuffers = [];

    public TransactionRouterClient(string[] natsUrl, int id)
    {
        string url = string.Join(',', natsUrl);
        var opts = NatsOpts.Default with { Url = url, Name = $"Router-{id}" };
        _nats = new NatsConnection(opts);
        _id = id;
        _batchRequest.RouterId = id;
    }

    public async ValueTask ConnectAsync()
    {
        await _nats.ConnectAsync();
        _cts = new CancellationTokenSource();
        _sendLoopTask = Task.Run(() => SendLoopAsync(_cts.Token));
    }

    private async Task SendLoopAsync(CancellationToken ct)
    {
        var reader = _sendChannel.Reader;
        try
        {
            while (await reader.WaitToReadAsync(ct))
            {
                foreach (var list in _pendingPubs.Values) list.Clear();

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
        }
        catch (OperationCanceledException) { }
        finally
        {
            while (reader.TryRead(out var payload)) ArrayPool<byte>.Shared.Return(payload.buffer);
        }
    }

    public void Publish(long accountId, string subject, IMessage message)
    {
        int size = message.CalculateSize();
        if (size == 0) return;

        byte[] buffer = ArrayPool<byte>.Shared.Rent(size);
        message.WriteTo(buffer.AsSpan(0, size));

        if (!_sendChannel.Writer.TryWrite(new SendPayload(subject, buffer, size, accountId)))
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    public IDisposable Subscribe<T>(string subject, Action<T, long> handler, Action<T>? resetAction = null)
        where T : class, IMessage, new()
    {
        string finalSubject = $"{subject}.{_id}";
        var cts = new CancellationTokenSource();
        _ctsList.TryAdd(cts, 0);

        // Khởi tạo Pool "chính chủ" Microsoft cho từng Subscriber
        var policy = new ProtobufPoolPolicy<T>(resetAction);
        var pool = _poolProvider.Create(policy);

        _ = SubscribeAsync(finalSubject, cts.Token, handler, pool);

        return new SubscriptionHandle(cts, c => _ctsList.TryRemove(c, out _));
    }

    private async Task SubscribeAsync<T>(string subject, CancellationToken ct, Action<T, long> handler, ObjectPool<T> pool)
        where T : class, IMessage, new()
    {
        var reusableBatch = new BatchTransactionRequest();
        try
        {
            // TỐI ƯU: Sử dụng NatsMemoryOwner để Zero GC hoàn toàn đầu nhận
            await foreach (var msg in _nats.SubscribeAsync<NatsMemoryOwner<byte>>(subject, cancellationToken: ct))
            {
                using (msg.Data) // Tự động dispose để trả memory về cho NATS
                {
                    if (msg.Data.Length == 0) continue;

                    reusableBatch.Payload.Clear();
                    reusableBatch.PlayerIds.Clear();
                    reusableBatch.MergeFrom(msg.Data.Memory.Span);

                    for (int i = 0; i < reusableBatch.PlayerIds.Count; i++)
                    {
                        var evt = pool.Get(); // Mượn từ Microsoft Pool
                        try
                        {
                            evt.MergeFrom(reusableBatch.Payload[i].Span);
                            handler(evt, reusableBatch.PlayerIds[i]);
                        }
                        finally
                        {
                            pool.Return(evt); // Trả về Microsoft Pool
                        }
                    }
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception) { /* Log retry logic here */ }
    }

    private async ValueTask FlushPendingAsync()
    {
        foreach (var entry in _pendingPubs)
        {
            if (entry.Value.Count == 0) continue;

            _batchRequest.Payload.Clear();
            _batchRequest.PlayerIds.Clear();
            _currentBatchBuffers.Clear();

            foreach (var p in entry.Value)
            {
                _batchRequest.Payload.Add(UnsafeByteOperations.UnsafeWrap(p.buffer.AsMemory(0, p.Length)));
                _batchRequest.PlayerIds.Add(p.PlayerId);
                _currentBatchBuffers.Add(p.buffer);

                if (_batchRequest.CalculateSize() > MaxBatchPayloadSize) await SendBatch(entry.Key);
            }
            if (_batchRequest.PlayerIds.Count > 0) await SendBatch(entry.Key);
        }
    }

    private async Task SendBatch(string subject)
    {
        int size = _batchRequest.CalculateSize();
        byte[] buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            _batchRequest.WriteTo(buffer.AsSpan(0, size));
            await _nats.PublishAsync(subject, buffer.AsMemory(0, size));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            foreach (var b in _currentBatchBuffers) ArrayPool<byte>.Shared.Return(b);
            _batchRequest.Payload.Clear();
            _batchRequest.PlayerIds.Clear();
            _currentBatchBuffers.Clear();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _sendChannel.Writer.TryComplete();
        _cts?.Cancel();
        if (_sendLoopTask != null) await _sendLoopTask;
        
        foreach (var c in _ctsList.Keys) { c.Cancel(); c.Dispose(); }
        await _nats.DisposeAsync();
    }

    private sealed class SubscriptionHandle(CancellationTokenSource cts, Action<CancellationTokenSource> dispose) : IDisposable
    {
        public void Dispose() { cts.Cancel(); dispose(cts); cts.Dispose(); }
    }
}