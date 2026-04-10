# TransactionRouter (RouterController)

README này hướng dẫn cách build, chạy và sử dụng các client trong dự án `TransactionRouter`.

## Mục đích

`TransactionRouter` là thư viện/ứng dụng đóng vai trò làm client để gửi và nhận các transaction giữa các service thông qua NATS, đồng thời lưu một số thông tin định tuyến tạm thời vào Redis.

Các lớp chính:
- `TransactionRouterClient` — client đơn giản để publish/subscribe các transaction trực tiếp tới router tương ứng.
- `TransactionServiceClient` — client bên service, chịu trách nhiệm gom batch các payload, tra cứu router qua Redis, gửi tới router và cập nhật heartbeat.

## Yêu cầu trước

- .NET SDK (target framework trong source là `net9.0`) — cài .NET 9 SDK hoặc phiên bản tương thích.
- NATS server (để publish/subscribe). Có thể dùng `nats-server` chính thức.
- Redis (StackExchange.Redis dùng để lưu định tuyến và heartbeat).

Tải và cài đặt .NET SDK: https://dotnet.microsoft.com

Chạy NATS server (ví dụ dùng Docker):

```powershell
docker run -p 4222:4222 -p 8222:8222 -d --name nats nats
```

Chạy Redis (ví dụ Docker):

```powershell
docker run -p 6379:6379 -d --name redis redis:6-alpine
```

## Build & test

Từ thư mục gốc của giải pháp (chứa `RouterController.sln`):

```powershell
dotnet restore
dotnet build -c Debug
dotnet test ./Test -c Debug
```

Nếu bạn chỉ muốn build project `TransactionRouter`:

```powershell
dotnet build ./TransactionRouter/TransactionRouter.csproj -c Debug
```

## Cấu trúc thư mục chính

- `TransactionRouter/` — source chính chứa `TransactionRouterClient.cs`, `TransactionServiceClient.cs` và `Protos/transaction_router.proto`.
- `Test/` — project tests.

## Sử dụng (ví dụ)

Dưới đây là ví dụ đơn giản minh họa cách sử dụng `TransactionRouterClient` và `TransactionServiceClient`.

Lưu ý: đoạn code ví dụ giả định bạn đã có message Protobuf tương ứng (được biên dịch vào namespace `TransactionRouter.Proto`).

Ví dụ: sử dụng `TransactionRouterClient` để publish và subscribe:

```csharp
using TransactionRouter;
using TransactionRouter.Proto; // namespace protobuf-generated

async Task RouterClientExample()
{
    var natsUrls = new[] { "nats://127.0.0.1:4222" };
    var client = new TransactionRouterClient(natsUrls, id: 1);
    await client.ConnectAsync();

    // Subscribe (synchronous handler)
    var sub = client.Subscribe<MyMessage>("topic.base", (msg, playerId) =>
    {
        Console.WriteLine($"Received for player {playerId}: {msg}");
    });

    // Publish
    var msg = new MyMessage { /* fill fields */ };
    client.Publish(accountId: 123, subject: "topic.base", message: msg);

    // Dispose when done
    sub.Dispose();
    await client.DisposeAsync();
}
```

Ví dụ: sử dụng `TransactionServiceClient` để publish và subscribe với Redis routing:

```csharp
using TransactionRouter;
using TransactionRouter.Proto;

async Task ServiceClientExample()
{
    var natsUrls = new[] { "nats://127.0.0.1:4222" };
    var redisUrl = "127.0.0.1:6379";

    var svc = new TransactionServiceClient(natsUrls, redisUrl);
    await svc.ConnectAsync();

    // Subscribe to incoming batches (service may act as consumer of other services)
    var sub = svc.Subscribe<MyMessage>("some.topic", (msg, playerId) =>
    {
        Console.WriteLine($"Service received message for player {playerId}");
    }, group: null);

    // Publish a message (it will be routed via Redis lookups)
    var msg = new MyMessage { /* fill fields */ };
    svc.Publish(playerId: 123, subject: "some.topic", message: msg);

    // Dispose when finished
    sub.Dispose();
    await svc.DisposeAsync();
}
```

## Cấu hình

- `TransactionRouterClient` constructor: `new TransactionRouterClient(string[] natsUrl, int id)` — `id` là router id gắn vào subject khi subscribe.
- `TransactionServiceClient` constructor: `new TransactionServiceClient(string[] natsUrls, string redisUrl)` — dùng Redis để lookup router id cho player khi gửi.

Các biến quan trọng trong code:
- `MaxBatchPayloadSize` — kích thước tối đa của batch (mặc định ~50KB).
- Redis key/sets: khoá heartbeat là `active_players_ts`.

## Ghi chú về hiệu năng và memory

- Code đã dùng nhiều tối ưu như:
  - ArrayPool để tái sử dụng buffer byte[]
  - Microsoft `ObjectPool<T>` (DefaultObjectPoolProvider) để tái sử dụng các object Protobuf và List tạm
  - NatsMemoryOwner để tránh phân mảnh GC khi nhận message

- Khi dùng API, hãy đảm bảo:
  - Gọi `Dispose()`/`DisposeAsync()` để giải phóng subscription và các task background.
  - Xử lý CancellationToken nếu bạn muốn dừng subscription sớm.

## Protobuf

Định nghĩa Protobuf nằm ở `TransactionRouter/Protos/transaction_router.proto`.

Nếu cần tái sinh C# classes từ .proto (nếu dự án chưa tự động làm), bạn có thể sử dụng `protoc` hoặc tích hợp `Google.Protobuf.Tools`/`Grpc.Tools` trong csproj. Thông thường project đã cấu hình để sinh mã khi build.

## Chạy local nhanh

1. Start NATS và Redis (xem ví dụ Docker ở trên).
2. Build và chạy một chương trình console nhỏ (ví dụ tạo project console tham chiếu `TransactionRouter` hoặc dùng test runner).

## Vấn đề phổ biến & Debug

- Nếu không nhận được message: kiểm tra subject (router id được thêm vào khi subscribe). Ví dụ `"topic.base.1"` nếu router id = 1.
- Nếu gặp lỗi Redis: kiểm tra `redisUrl` và trạng thái kết nối (đầu ra log nếu thêm).
- Để kiểm tra NATS: dùng `nats` CLI hoặc web monitoring port của container.

## Liên hệ

Nếu cần cập nhật README hoặc có yêu cầu hỗ trợ thêm, gửi thông tin chi tiết về lỗi hoặc mục tiêu bạn muốn đạt được.

---

Tập tin liên quan:
- `TransactionRouter/TransactionRouterClient.cs`
- `TransactionRouter/TransactionServiceClient.cs`
- `TransactionRouter/Protos/transaction_router.proto`

