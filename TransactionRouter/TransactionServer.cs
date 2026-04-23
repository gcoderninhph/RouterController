using Google.Protobuf;
using Natify; // Thư viện mới
using StackExchange.Redis;

namespace TransactionRouter;

public class TransactionServer : IAsyncDisposable
{
    // private readonly NatsConnection _nats;
    private readonly NatifyServer _natifyServer;
    private readonly string _redisUrl;
    private ConnectionMultiplexer? _redisConnection;
    private IDatabase? _redisDb;
    
    

    public TransactionServer(string[] natsUrls, string redisUrl, string serverName, string clientName)
    {
        string url = string.Join(',', natsUrls);
        _redisUrl = redisUrl;
        _natifyServer = new NatifyServer(url, serverName, "Worker", clientName);
        _redisConnection = ConnectionMultiplexer.ConnectAsync(_redisUrl).GetAwaiter().GetResult();
        _redisDb = _redisConnection.GetDatabase();
    }
    
    public void Publish(string topic,long playerId,  IMessage message)
    {
        
        
        
        // _natifyServer.Publish(topic, );
    }

    public IDisposable Subscribe<T>(string subject, Action<T, long> handler, string? group = null,
        Action<T>? resetAction = null)
        where T : class, IMessage, new()
    {
        
    }

    public IDisposable Subscribe<T>(string subject, Func<T, long, Task> handler, string? group = null,
        Action<T>? resetAction = null)
        where T : class, IMessage, new()
    {
        
    }

    public async ValueTask DisposeAsync()
    {

    }
}