using Google.Protobuf;
using Natify; // Thư viện mới
using StackExchange.Redis;
using TransactionRouter.Proto;

namespace TransactionRouter;

public class TransactionServer : IDisposable
{
    // private readonly NatsConnection _nats;
    private readonly NatifyServer _natifyServer;
    private readonly string _redisUrl;
    private readonly ConnectionMultiplexer _redisConnection;
    private readonly IDatabase _redisDb;

    private string _redisPrefix;
    private string _sortSetTimeOutName;

    private CancellationTokenSource _ct;

    private static readonly TimeSpan TimeOut = TimeSpan.FromSeconds(10);


    public TransactionServer(string natsUrl, string redisUrl, string serverName, string clientName)
    {
        _redisUrl = redisUrl;
        _natifyServer = new NatifyServer(natsUrl, serverName, "Worker", clientName);
        _redisConnection = ConnectionMultiplexer.ConnectAsync(_redisUrl).GetAwaiter().GetResult();
        _redisDb = _redisConnection.GetDatabase();
        _redisPrefix = $"{serverName}_pr:";
        _sortSetTimeOutName = $"{serverName}_timeout";
        _ct = new CancellationTokenSource();

        _ = Task.Run(CleanUpTimeoutPlayers, _ct.Token);
    }

    private async Task CleanUpTimeoutPlayers()
    {
        while (!_ct.Token.IsCancellationRequested)
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var timeoutPlayerIds =
                await _redisDb.SortedSetRangeByScoreAsync(_sortSetTimeOutName, stop: now - (long)TimeOut.TotalSeconds);
            if (timeoutPlayerIds.Length > 0)
            {
                foreach (var playerId in timeoutPlayerIds)
                {
                    await _redisDb.KeyDeleteAsync($"{_redisPrefix}{playerId}");
                    await _redisDb.SortedSetRemoveAsync(_sortSetTimeOutName, playerId);
                }
            }

            await Task.Delay(TimeSpan.FromSeconds(5), _ct.Token);
        }
    }


    public async void Publish(string topic, long playerId, IMessage message)
    {
        try
        {
            var regionId = await _redisDb.StringGetAsync($"{_redisPrefix}{playerId}");
            if (regionId.HasValue)
            {
                _natifyServer.Publish(topic, regionId.ToString(), new TransactionRequest
                {
                    PlayerId = playerId,
                    TransactionData = ByteString.CopyFrom(message.ToByteArray())
                });
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error publishing message for player {playerId}: {ex.Message}");
        }
    }

    public void OnRequest<TReq, TRes>(string topic, Func<TReq, Task<TRes>> handler)
        where TReq : class, IMessage, new()
        where TRes : class, IMessage, new()
    {
        _natifyServer.OnRequest<TReq, TRes>(topic, async request =>
        {
            var result = await handler(request.request);
            return result;
        });
    }

    public void Subscribe<T>(string subject, Action<T, long> handler)
        where T : class, IMessage, new()
    {
        _natifyServer.OnMessage<TransactionRequest>(subject, async message =>
        {
            var data = new T();
            data.MergeFrom(message.data.Value.TransactionData);
            handler(data, message.data.Value.PlayerId);

            await _redisDb.StringSetAsync($"{_redisPrefix}{message.data.Value.PlayerId}", message.regionId);
            await _redisDb.SortedSetAddAsync(_sortSetTimeOutName, message.data.Value.PlayerId,
                DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        });
    }

    public void Subscribe<T>(string subject, Func<T, long, Task> handler)
        where T : class, IMessage, new()
    {
        _natifyServer.OnMessage<TransactionRequest>(subject, async message =>
        {
            var data = new T();
            data.MergeFrom(message.data.Value.TransactionData);
            await handler(data, message.data.Value.PlayerId);
            await _redisDb.StringSetAsync($"{_redisPrefix}{message.data.Value.PlayerId}", message.regionId);
            await _redisDb.SortedSetAddAsync(_sortSetTimeOutName, message.data.Value.PlayerId,
                DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        });
    }

    public void Dispose()
    {
        _redisConnection.Dispose();
        _natifyServer.Dispose();
        _ct.Cancel();
        _ct.Dispose();
    }
}