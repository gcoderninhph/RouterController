using System.Buffers;
using System.Collections.Concurrent;
using System.Threading.Channels;
using Google.Protobuf;
using Microsoft.Extensions.ObjectPool;
using Natify; // Thư viện mới
using NATS.Client.Core;
using TransactionRouter.Proto;

namespace TransactionRouter;

public class TransactionClient : IDisposable
{
    private readonly NatifyClientFast _natify;
    private readonly string _id;

    public string Id => _id;


    public TransactionClient(string natsUrl, string clientName, string serverName)
    {
        _id = Guid.NewGuid().ToString("N");
        _natify = new NatifyClientFast(natsUrl, clientName, "Workers", _id, serverName);
    }


    public void Publish( string topic,long accountId, IMessage message)
    {
        _natify.Publish(topic, new TransactionRequest
        {
            PlayerId = accountId,
            TransactionData = ByteString.CopyFrom(message.ToByteArray())
        });
    }

    public void Subscribe<T>(string topic, Action<T, long> handler)
        where T : class, IMessage, new()
    {
        _natify.OnMessage<TransactionRequest>(topic, (data) =>
            {
                var message = new T();
                message.MergeFrom(data.Value.TransactionData);
                handler(message, data.Value.PlayerId);
            }
        );
    }

    public void Subscribe<T>(string topic, Func<T, long, Task> handler)
        where T : class, IMessage, new()
    {
        _natify.OnMessage<TransactionRequest>(topic, async data =>
            {
                var message = new T();
                message.MergeFrom(data.Value.TransactionData);
                await handler(message, data.Value.PlayerId);
            }
        );
    }


    public void Dispose()
    {
        _natify.Dispose();
    }
}