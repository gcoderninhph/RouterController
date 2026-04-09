using System.Buffers;
using System.Buffers.Text;
using System.Threading.Channels;
using Google.Protobuf;
using NATS.Client.Core;
using StackExchange.Redis;
using TransactionRouter.Proto;
using Gcoder.Poll;

namespace TransactionRouter;

public class TransactionServiceClient : IAsyncDisposable
{
    private readonly struct SendPayload(string subject, byte[] buffer, int length, long playerId)
    {
        public readonly string Subject = subject;
        public readonly byte[] Buffer = buffer;
        public readonly int Length = length;
        public readonly long PlayerId = playerId;
    }

    private readonly NatsConnection _nats;
    private readonly string _redisUrl;
    private ConnectionMultiplexer? _redisConnection;
    private IDatabase? _redisDb;

    private Task? _sendLoopTask;
    private Task? _cleanupLoopTask;
    private CancellationTokenSource? _cts;

    private const int MaxBatchPayloadSize = 50 * 1024;
    private const string HeartbeatSetKey = "active_players_ts";

    private readonly List<CancellationTokenSource> _ctsList = [];
    private readonly object _ctsListLock = new();

    private readonly Channel<SendPayload> _sendChannel =
        Channel.CreateUnbounded<SendPayload>(new UnboundedChannelOptions { SingleReader = true });

    // Các bộ nhớ dùng chung để tái sử dụng (Zero-GC)
    private readonly Dictionary<int, Dictionary<string, List<SendPayload>>> _pendingPubs = [];
    private readonly Dictionary<long, int> _routeMap = [];
    private readonly BatchTransactionRequest _batchRequest = new();
    private readonly List<byte[]> _currentBatchBuffers = [];

    private readonly ObjectPool<List<SendPayload>> _listPool = new(
        resetAction: list => list.Clear(),
        lengthDefault: 50
    );

    public TransactionServiceClient(string[] natsUrls, string redisUrl)
    {
        string url = string.Join(',', natsUrls);
        var opts = NatsOpts.Default with { Url = url, Name = "TransactionServiceClient" };
        _nats = new NatsConnection(opts);
        _redisUrl = redisUrl;
    }

    public async ValueTask ConnectAsync()
    {
        await _nats.ConnectAsync();
        _redisConnection = await ConnectionMultiplexer.ConnectAsync(_redisUrl);
        _redisDb = _redisConnection.GetDatabase();

        _cts = new CancellationTokenSource();
        StartSendLoop();
        StartCleanupLoop();
    }

    private void StartSendLoop()
    {
        _sendLoopTask = Task.Factory.StartNew(
            () => SendLoopAsync(_cts!.Token), _cts!.Token,
            TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
    }

    private void StartCleanupLoop()
    {
        _cleanupLoopTask = Task.Factory.StartNew(
            () => CleanupLoopAsync(_cts!.Token), _cts!.Token,
            TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
    }

    // Tiện ích tạo RedisKey mà không tạo String rác
    private static void WriteRedisKey(Span<byte> buffer, long playerId, out int bytesWritten)
    {
        // "pr:" ASCII
        buffer[0] = 112;
        buffer[1] = 114;
        buffer[2] = 58;
        Utf8Formatter.TryFormat(playerId, buffer[3..], out int written);
        bytesWritten = 3 + written;
    }

    private async Task SendLoopAsync(CancellationToken ct)
    {
        var reader = _sendChannel.Reader;
        var currentPayloads = new List<SendPayload>();
        var distinctPlayerIds = new HashSet<long>();

        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    await reader.WaitToReadAsync(ct);
                    currentPayloads.Clear();
                    distinctPlayerIds.Clear();

                    while (reader.TryRead(out var payload))
                    {
                        currentPayloads.Add(payload);
                        distinctPlayerIds.Add(payload.PlayerId);
                    }

                    int pCount = distinctPlayerIds.Count;
                    if (currentPayloads.Count > 0 && pCount > 0)
                    {
                        var playerIdsArray = ArrayPool<long>.Shared.Rent(pCount);
                        distinctPlayerIds.CopyTo(playerIdsArray);

                        await ProcessAndRouteBatchAsync(currentPayloads, playerIdsArray, pCount);

                        // Cập nhật Heartbeat (Task này tự trả mảng về Pool qua ContinueWith)
                        UpdateHeartbeatsAsync(playerIdsArray, pCount);

                        ArrayPool<long>.Shared.Return(playerIdsArray);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception)
                {
                }
            }
        }
        finally
        {
            while (reader.TryRead(out var payload)) ArrayPool<byte>.Shared.Return(payload.Buffer);
        }
    }

    private async Task ProcessAndRouteBatchAsync(List<SendPayload> payloads, long[] playerIdsArray, int pCount)
    {
        if (_redisDb == null) return;

        var redisKeys = ArrayPool<RedisKey>.Shared.Rent(pCount);
        Span<byte> keyBuffer = stackalloc byte[32];

        for (int i = 0; i < pCount; i++)
        {
            WriteRedisKey(keyBuffer, playerIdsArray[i], out int len);
            redisKeys[i] =
                (RedisKey)keyBuffer[..len]
                    .ToArray(); // Chấp nhận ToArray ở đây vì RedisKey struct cần copy dữ liệu Span
        }

        // Padding mảng để tránh rác ở cuối khi gửi lên Redis
        var lastValidKey = redisKeys[pCount - 1];
        for (int i = pCount; i < redisKeys.Length; i++) redisKeys[i] = lastValidKey;

        RedisValue[] routerIds = await _redisDb.StringGetAsync(redisKeys);
        ArrayPool<RedisKey>.Shared.Return(redisKeys);

        _routeMap.Clear();
        for (int i = 0; i < pCount; i++)
        {
            if (routerIds[i].HasValue && routerIds[i].TryParse(out int routerId))
                _routeMap[playerIdsArray[i]] = routerId;
        }

        // Dọn dẹp pending cũ
        foreach (var routerDict in _pendingPubs.Values)
        {
            foreach (var list in routerDict.Values) _listPool.Return(list);
            routerDict.Clear();
        }

        // Gom Batch
        foreach (var payload in payloads)
        {
            if (_routeMap.TryGetValue(payload.PlayerId, out int routerId))
            {
                if (!_pendingPubs.TryGetValue(routerId, out var subjectDict))
                {
                    subjectDict = [];
                    _pendingPubs[routerId] = subjectDict;
                }

                if (!subjectDict.TryGetValue(payload.Subject, out var list))
                {
                    list = _listPool.Rent();
                    subjectDict[payload.Subject] = list;
                }

                list.Add(payload);
            }
            else
            {
                ArrayPool<byte>.Shared.Return(payload.Buffer);
            }
        }

        foreach (var routerEntry in _pendingPubs)
        {
            foreach (var subjectEntry in routerEntry.Value)
            {
                if (subjectEntry.Value.Count > 0)
                    await FlushToRouterAsync(routerEntry.Key, subjectEntry.Key, subjectEntry.Value);
            }
        }
    }

    private async Task FlushToRouterAsync(int routerId, string baseSubject, List<SendPayload> payloadList)
    {
        string finalSubject = $"{baseSubject}.{routerId}";
        _batchRequest.Payload.Clear();
        _batchRequest.PlayerIds.Clear();
        _currentBatchBuffers.Clear();

        foreach (var payload in payloadList)
        {
            _batchRequest.Payload.Add(UnsafeByteOperations.UnsafeWrap(payload.Buffer.AsMemory(0, payload.Length)));
            _batchRequest.PlayerIds.Add(payload.PlayerId);
            _currentBatchBuffers.Add(payload.Buffer);

            if (_batchRequest.CalculateSize() > MaxBatchPayloadSize)
                await SendCurrentBatch(finalSubject);
        }

        if (_batchRequest.PlayerIds.Count > 0)
            await SendCurrentBatch(finalSubject);
    }

    private async Task SendCurrentBatch(string finalSubject)
    {
        int size = _batchRequest.CalculateSize();
        byte[] buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            _batchRequest.WriteTo(buffer.AsSpan(0, size));
            await _nats.PublishAsync(finalSubject, buffer.AsMemory(0, size));
        }
        catch
        {
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

    private void UpdateHeartbeatsAsync(long[] playerIds, int count)
    {
        if (_redisDb == null || count == 0) return;

        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var entries = ArrayPool<SortedSetEntry>.Shared.Rent(count);

        for (int i = 0; i < count; i++) entries[i] = new SortedSetEntry(playerIds[i], now);

        var lastValid = entries[count - 1];
        for (int i = count; i < entries.Length; i++) entries[i] = lastValid;

        _ = _redisDb.SortedSetAddAsync(HeartbeatSetKey, entries).ContinueWith(
            (task, state) => { ArrayPool<SortedSetEntry>.Shared.Return((SortedSetEntry[])state!); }, entries);
    }

    public void Publish(long playerId, string subject, IMessage message)
    {
        int size = message.CalculateSize();
        if (size == 0) return;

        byte[] buffer = ArrayPool<byte>.Shared.Rent(size);
        message.WriteTo(buffer.AsSpan(0, size));

        if (!_sendChannel.Writer.TryWrite(new SendPayload(subject, buffer, size, playerId)))
            ArrayPool<byte>.Shared.Return(buffer);
    }

    public IDisposable Subscribe<T>(string subject, Action<T, long> handler, Action<T> resetAction)
        where T : IMessage, new()
    {
        var cts = new CancellationTokenSource();
        var ct = cts.Token;
        lock (_ctsListLock)
        {
            _ctsList.Add(cts);
        }

        var eventPool = new ObjectPool<T>(resetAction, 100);
        _ = SubscribeAsync(subject, ct, handler, eventPool);

        return new SubscriptionHandle(cts, RemoveCts);
    }

    private async Task SubscribeAsync<T>(string subject, CancellationToken ct, Action<T, long> handler,
        ObjectPool<T> eventPool)
        where T : IMessage, new()
    {
        var reusableBatchRequest = new BatchTransactionRequest();

        await foreach (var msg in _nats.SubscribeAsync<NatsMemoryOwner<byte>>(subject, cancellationToken: ct))
        {
            using (msg.Data)
            {
                if (msg.Data.Length == 0 || _redisDb == null) continue;
                try
                {
                    Span<byte> keyBuffer = stackalloc byte[32];
                    reusableBatchRequest.Payload.Clear();
                    reusableBatchRequest.PlayerIds.Clear();
                    reusableBatchRequest.MergeFrom(msg.Data.Memory.Span);

                    int pCount = reusableBatchRequest.PlayerIds.Count;
                    int routerId = reusableBatchRequest.RouterId;

                    if (pCount > 0)
                    {
                        var redisBatchUpdate = ArrayPool<KeyValuePair<RedisKey, RedisValue>>.Shared.Rent(pCount);
                        for (int i = 0; i < pCount; i++)
                        {
                            WriteRedisKey(keyBuffer, reusableBatchRequest.PlayerIds[i], out int len);
                            redisBatchUpdate[i] =
                                new KeyValuePair<RedisKey, RedisValue>((RedisKey)keyBuffer[..len].ToArray(), routerId);
                        }

                        var lastValid = redisBatchUpdate[pCount - 1];
                        for (int i = pCount; i < redisBatchUpdate.Length; i++) redisBatchUpdate[i] = lastValid;

                        _ = _redisDb.StringSetAsync(redisBatchUpdate).ContinueWith((t, s) =>
                            ArrayPool<KeyValuePair<RedisKey, RedisValue>>.Shared.Return(
                                (KeyValuePair<RedisKey, RedisValue>[])s!), redisBatchUpdate);

                        for (var i = 0; i < pCount; i++)
                        {
                            T evt = eventPool.Rent();
                            evt.MergeFrom(reusableBatchRequest.Payload[i].Span);
                            handler(evt, reusableBatchRequest.PlayerIds[i]);
                            eventPool.Return(evt);
                        }
                    }
                }
                catch
                {
                }
            }
        }
    }

    private async Task CleanupLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(5000, ct);
                if (_redisDb == null) continue;

                long expiredThreshold = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 10;
                var expiredPlayers = await _redisDb.SortedSetRangeByScoreAsync(HeartbeatSetKey, 0, expiredThreshold);
                int pCount = expiredPlayers.Length;

                if (pCount > 0)
                {
                    Span<byte> keyBuffer = stackalloc byte[32];

                    var keysToDelete = ArrayPool<RedisKey>.Shared.Rent(pCount);
                    for (int i = 0; i < pCount; i++)
                    {
                        WriteRedisKey(keyBuffer, (long)expiredPlayers[i], out int len);
                        keysToDelete[i] = (RedisKey)keyBuffer[..len].ToArray();
                    }

                    var last = keysToDelete[pCount - 1];
                    for (int i = pCount; i < keysToDelete.Length; i++) keysToDelete[i] = last;

                    await _redisDb.KeyDeleteAsync(keysToDelete);
                    await _redisDb.SortedSetRemoveAsync(HeartbeatSetKey, expiredPlayers);
                    ArrayPool<RedisKey>.Shared.Return(keysToDelete);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
            }
        }
    }

    private void RemoveCts(CancellationTokenSource cts)
    {
        lock (_ctsListLock) _ctsList.Remove(cts);
    }

    public async ValueTask DisposeAsync()
    {
        _sendChannel.Writer.TryComplete();
        if (_cts != null) _cts.Cancel();
        if (_sendLoopTask != null)
            try
            {
                await _sendLoopTask;
            }
            catch
            {
            }

        if (_cleanupLoopTask != null)
            try
            {
                await _cleanupLoopTask;
            }
            catch
            {
            }

        if (_cts != null) _cts.Dispose();

        CancellationTokenSource[] snapShot;
        int count;
        lock (_ctsListLock)
        {
            count = _ctsList.Count;
            snapShot = ArrayPool<CancellationTokenSource>.Shared.Rent(count);
            _ctsList.CopyTo(snapShot, 0);
            _ctsList.Clear();
        }

        for (int i = 0; i < count; i++)
        {
            try
            {
                snapShot[i]?.Cancel();
            }
            catch
            {
            }

            snapShot[i]?.Dispose();
        }

        ArrayPool<CancellationTokenSource>.Shared.Return(snapShot, true);

        await _nats.DisposeAsync();
        if (_redisConnection != null) await _redisConnection.DisposeAsync();
    }

    private sealed class SubscriptionHandle(CancellationTokenSource cts, Action<CancellationTokenSource> onDispose)
        : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            try
            {
                cts.Cancel();
            }
            catch
            {
            }

            onDispose(cts);
            cts.Dispose();
        }
    }
}