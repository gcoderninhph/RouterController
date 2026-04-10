using System.Collections.Concurrent;
using System.Diagnostics;
using Google.Protobuf.WellKnownTypes; // Dùng StringValue để test
using NUnit.Framework;
using StackExchange.Redis;


namespace TransactionRouter.Tests
{
    [TestFixture]
    public class TransactionSystemIntegrationTests
    {
        private const string NatsUrl = "nats://localhost:4222";
        private const string RedisUrl = "localhost:6379";
        private ConnectionMultiplexer _redis;

        [OneTimeSetUp]
        public async Task GlobalSetup()
        {
            var options = ConfigurationOptions.Parse(RedisUrl);
            options.AllowAdmin = true; // <--- KÍCH HOẠT QUYỀN ADMIN Ở ĐÂY

            _redis = await ConnectionMultiplexer.ConnectAsync(options);
        }

        [SetUp]
        public async Task SetUp()
        {
            // Xoá toàn bộ data trên Redis Db0 để test case không bị nhiễu chéo
            var endpoints = _redis.GetEndPoints();
            var server = _redis.GetServer(endpoints[0]);
            await server.FlushDatabaseAsync();
        }

        [OneTimeTearDown]
        public async Task GlobalTeardown()
        {
            await _redis.DisposeAsync();
        }

        // =========================================================================================
        // CASE 1: Basic E2E (Ping Pong) - Đảm bảo Service nhận được từ Router và ngược lại
        // =========================================================================================
        [Test]
        public async Task Basic_PingPong_E2E_Success()
        {
            await using var service = new TransactionServiceClient(new[] { NatsUrl }, RedisUrl);
            await using var router = new TransactionRouterClient(new[] { NatsUrl }, id: 1);

            await service.ConnectAsync();
            await router.ConnectAsync();

            var tcsServiceReceive = new TaskCompletionSource<bool>();
            var tcsRouterReceive = new TaskCompletionSource<bool>();
            long testPlayerId = 1001;

            // 1. Service lắng nghe "auth.login" từ Router (Đồng thời học được Route vào Redis)
            using var sub1 = service.Subscribe<StringValue>("auth.login", (msg, pId) =>
            {
                Assert.That(pId, Is.EqualTo(testPlayerId));
                Assert.That(msg.Value, Is.EqualTo("HelloService"));
                tcsServiceReceive.TrySetResult(true);
            }, null, msg => msg.Value = string.Empty);

            // 2. Router lắng nghe "game.events" từ Service
            using var sub2 = router.Subscribe<StringValue>("game.events", (msg, pId) =>
            {
                Assert.That(pId, Is.EqualTo(testPlayerId));
                Assert.That(msg.Value, Is.EqualTo("HelloRouter"));
                tcsRouterReceive.TrySetResult(true);
            });

            // Hành động: Router gửi Service -> Service học Route
            router.Publish(testPlayerId, "auth.login", new StringValue { Value = "HelloService" });

            // Đợi Service nhận được
            await Task.WhenAny(tcsServiceReceive.Task, Task.Delay(2000));
            Assert.That(tcsServiceReceive.Task.IsCompletedSuccessfully, Is.True, "Service không nhận được message.");

            // Đợi 1 chút để Service đẩy _redisDb.StringSetAsync (học route) xong ở background
            await Task.Delay(200);

            // Hành động: Service phản hồi lại Router
            service.Publish(testPlayerId, "game.events", new StringValue { Value = "HelloRouter" });

            // Đợi Router nhận được
            await Task.WhenAny(tcsRouterReceive.Task, Task.Delay(2000));
            Assert.That(tcsRouterReceive.Task.IsCompletedSuccessfully, Is.True,
                "Router không nhận được message do Routing sai.");
        }

        // =========================================================================================
        // CASE 2: HIGH THROUGHPUT & STRESS TEST (Case cực nặng, gửi 100.000 messages)
        // =========================================================================================
        [Test]
        public async Task Massive_Throughput_100k_Requests_StressTest()
        {
            await using var service = new TransactionServiceClient(new[] { NatsUrl }, RedisUrl);
            await using var router = new TransactionRouterClient(new[] { NatsUrl }, id: 99);

            await service.ConnectAsync();
            await router.ConnectAsync();

            const int totalMessages = 100_000;
            const int totalPlayers = 5000;

            var receivedCount = 0;
            var countdown = new CountdownEvent(totalMessages);

            // Subscriber: Router lắng nghe
            using var sub = router.Subscribe<StringValue>("stress.test", (msg, pId) =>
            {
                Interlocked.Increment(ref receivedCount);
                countdown.Signal();
            });

            // Fake Route trong Redis trước để Service biết đường gửi (bypass bước Router gửi lên Service)
            var db = _redis.GetDatabase();
            var redisKeys = new KeyValuePair<RedisKey, RedisValue>[totalPlayers];
            for (int i = 0; i < totalPlayers; i++)
            {
                redisKeys[i] = new KeyValuePair<RedisKey, RedisValue>($"pr:{i}", 99);
            }

            await db.StringSetAsync(redisKeys);

            // Bắn phá 100,000 tin nhắn bằng đa luồng
            var sw = Stopwatch.StartNew();
            Parallel.For(0, totalMessages, i =>
            {
                long playerId = i % totalPlayers; // Chia đều cho 5000 players
                service.Publish(playerId, "stress.test", new StringValue { Value = $"Msg_{i}" });
            });

            // Chờ nhận đủ 100k tin nhắn hoặc timeout sau 15 giây
            bool allReceived = countdown.Wait(TimeSpan.FromSeconds(15));
            sw.Stop();

            Console.WriteLine(
                $"Stress Test: Sent & Received {receivedCount}/{totalMessages} in {sw.ElapsedMilliseconds}ms");

            Assert.That(allReceived, Is.True,
                $"Chỉ nhận được {receivedCount}/{totalMessages} messages. Bị drop hoặc dead lock!");
        }

        // =========================================================================================
        // CASE 3: TEST VƯỢT QUÁ MAX BATCH SIZE (Trigger Auto-Flush)
        // =========================================================================================
        [Test]
        public async Task Exceed_MaxBatchSize_Should_Trigger_Immediate_Flush()
        {
            // Khởi tạo cả Service và Router
            await using var service = new TransactionServiceClient(new[] { NatsUrl }, RedisUrl);
            await using var router = new TransactionRouterClient(new[] { NatsUrl }, id: 2);

            await service.ConnectAsync();
            await router.ConnectAsync();

            int received = 0;
            var tcs = new TaskCompletionSource<bool>();

            // FIX: Dùng Service để Subscribe. 
            // Theo logic, Service sẽ lắng nghe đúng kênh gốc ("large.payload")
            using var sub = service.Subscribe<StringValue>("large.payload", (msg, pId) =>
            {
                Interlocked.Increment(ref received);
                if (received == 2) tcs.TrySetResult(true);
            }, null, msg => msg.Value = string.Empty);

            // MaxBatchPayloadSize = 50 * 1024 (50KB)
            // Tạo 1 string khoảng 30KB
            string hugeString = new string('A', 30 * 1024);

            // Gửi 2 message x 30KB = 60KB. Vượt 50KB -> Sẽ ép Trigger SendCurrentBatch ngay ở vòng lặp
            router.Publish(1, "large.payload", new StringValue { Value = hugeString });
            router.Publish(2, "large.payload", new StringValue { Value = hugeString });

            // Đợi tối đa 2 giây
            await Task.WhenAny(tcs.Task, Task.Delay(2000));

            Assert.That(received, Is.EqualTo(2), "Service không nhận được batch khi bị ép flush!");
        }

        // =========================================================================================
        // CASE 4: MULTI-ROUTER CORRECT ROUTING (Kiểm tra định tuyến chính xác nhiều Router)
        // =========================================================================================
        [Test]
        public async Task MultiRouter_Should_Route_To_Correct_RouterClient()
        {
            await using var service = new TransactionServiceClient(new[] { NatsUrl }, RedisUrl);
            await using var router1 = new TransactionRouterClient(new[] { NatsUrl }, id: 10);
            await using var router2 = new TransactionRouterClient(new[] { NatsUrl }, id: 20);

            await service.ConnectAsync();
            await router1.ConnectAsync();
            await router2.ConnectAsync();

            var r1Received = new ConcurrentBag<long>();
            var r2Received = new ConcurrentBag<long>();

            using var subR1 = router1.Subscribe<StringValue>("multi.route", (msg, pId) => r1Received.Add(pId));
            using var subR2 = router2.Subscribe<StringValue>("multi.route", (msg, pId) => r2Received.Add(pId));

            // Set Route thủ công
            var db = _redis.GetDatabase();
            await db.StringSetAsync($"pr:100", 10); // Player 100 -> Router 10
            await db.StringSetAsync($"pr:200", 20); // Player 200 -> Router 20

            // Service gửi
            service.Publish(100, "multi.route", new StringValue { Value = "A" });
            service.Publish(200, "multi.route", new StringValue { Value = "B" });
            service.Publish(100, "multi.route", new StringValue { Value = "C" });

            await Task.Delay(500); // Chờ NATS chuyển phát

            Assert.That(r1Received.Count, Is.EqualTo(2), "Router 1 phải nhận 2 messages của player 100");
            Assert.That(r1Received.All(x => x == 100), Is.True);

            Assert.That(r2Received.Count, Is.EqualTo(1), "Router 2 phải nhận 1 messages của player 200");
            Assert.That(r2Received.All(x => x == 200), Is.True);
        }

        // =========================================================================================
        // CASE 5: DISPOSE & CANCELLATION (Đảm bảo không crash ngầm hay treo khi Shutdown)
        // =========================================================================================
        [Test]
        public async Task Graceful_Shutdown_Should_Not_Deadlock()
        {
            var router = new TransactionRouterClient(new[] { NatsUrl }, id: 5);
            await router.ConnectAsync();

            var handle = router.Subscribe<StringValue>("shutdown.test", (msg, pId) => { });

            // Bơm data vào channel liên tục nhưng ko await
            for (int i = 0; i < 1000; i++)
            {
                router.Publish(1, "shutdown.test", new StringValue { Value = "Ping" });
            }

            // Lập tức gọi Dispose trong khi Channel vẫn đang xử lý Batching
            var disposeTask = router.DisposeAsync().AsTask();

            var result = await Task.WhenAny(disposeTask, Task.Delay(3000));
            Assert.That(result, Is.EqualTo(disposeTask),
                "Dispose bị treo (Deadlock) ở SendLoop hoặc Dispose CancellationToken!");
        }

        // =========================================================================================
        // CASE 6: HEARTBEAT REDIS (Đảm bảo Service update active players vào SortedSet)
        // =========================================================================================
        [Test]
        public async Task Heartbeat_Should_Update_To_Redis_SortedSet()
        {
            await using var service = new TransactionServiceClient(new[] { NatsUrl }, RedisUrl);
            await service.ConnectAsync();

            // Fake route để Service không bỏ qua gói tin (Vì nếu route rỗng, buffer bị trả về pool ngay lập tức)
            await _redis.GetDatabase().StringSetAsync($"pr:555", 1);

            // Gửi 1 gói để trigger UpdateHeartbeatsAsync
            service.Publish(555, "any.subject", new StringValue { Value = "Ping" });

            // Background thread đẩy redis, cần đợi tí
            await Task.Delay(500);

            var score = await _redis.GetDatabase().SortedSetScoreAsync("active_players_ts", 555);
            Assert.That(score.HasValue, Is.True, "Redis Heartbeat SortedSet không được cập nhật.");
            Assert.That(score.Value, Is.GreaterThan(DateTimeOffset.UtcNow.AddMinutes(-1).ToUnixTimeSeconds()));
        }

        // =========================================================================================
        // CASE: PERFORMANCE BENCHMARK (1K, 10K, 100K Requests)
        // =========================================================================================
        [TestCase(1_000, 100, "1K Requests")]
        [TestCase(10_000, 1_000, "10K Requests")]
        [TestCase(100_000, 5_000, "100K Requests")]
        public async Task Performance_Throughput_Benchmarks(int totalMessages, int totalPlayers, string testName)
        {
            // 1. Khởi tạo
            await using var service = new TransactionServiceClient(new[] { NatsUrl }, RedisUrl);
            await using var router = new TransactionRouterClient(new[] { NatsUrl }, id: 99);

            await service.ConnectAsync();
            await router.ConnectAsync();

            var db = _redis.GetDatabase();
            int receivedCount = 0;
            var countdown = new CountdownEvent(totalMessages);
            var tcs = new TaskCompletionSource<bool>();

            // 2. Router đăng ký nhận sự kiện từ Service
            using var sub = router.Subscribe<StringValue>("benchmark.events", (msg, pId) =>
            {
                var current = Interlocked.Increment(ref receivedCount);
                if (current == totalMessages)
                {
                    tcs.TrySetResult(true);
                }
            });

            // 3. Khởi tạo dữ liệu giả lập trên Redis: Phân bổ đều Player vào Router 99
            // Làm bước này để Service có thể lấy Route ngay lập tức mà không bị drop gói tin
            var redisKeys = new KeyValuePair<RedisKey, RedisValue>[totalPlayers];
            for (int i = 0; i < totalPlayers; i++)
            {
                redisKeys[i] = new KeyValuePair<RedisKey, RedisValue>($"pr:{i}", 99);
            }

            await db.StringSetAsync(redisKeys);

            // Warm-up NATS connection (gửi mồi 1 tin nhắn để thiết lập đường truyền mạng)
            service.Publish(0, "benchmark.events", new StringValue { Value = "Warmup" });
            await Task.Delay(500);
            // Reset lại biến đếm sau quá trình warm-up
            Interlocked.Exchange(ref receivedCount, 0);

            // 4. BẮT ĐẦU BENCHMARK
            var sw = Stopwatch.StartNew();

            // Dùng Parallel.For để mô phỏng tải đa luồng thực tế ép vào Channel
            Parallel.For(0, totalMessages, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                i =>
                {
                    long playerId = i % totalPlayers; // Round-robin chia đều người chơi
                    service.Publish(playerId, "benchmark.events", new StringValue { Value = "B" });
                });

            // 5. Đợi kết quả (Timeout linh hoạt: 5 giây cho 1k, 10 giây cho 10k, 30 giây cho 100k)
            int timeoutSeconds = totalMessages > 50000 ? 30 : 10;
            bool completed = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(timeoutSeconds))) == tcs.Task;

            sw.Stop();

            // 6. In Report ra Console / Test Output
            double elapsedMs = sw.Elapsed.TotalMilliseconds;
            double rps = (totalMessages / elapsedMs) * 1000;

            TestContext.WriteLine("=========================================");
            TestContext.WriteLine($"[{testName}] Báo cáo Hiệu suất:");
            TestContext.WriteLine($" - Số lượng gửi   : {totalMessages:N0} reqs");
            TestContext.WriteLine($" - Số lượng nhận  : {receivedCount:N0} reqs");
            TestContext.WriteLine($" - Thời gian xử lý: {elapsedMs:N2} ms");
            TestContext.WriteLine($" - Thông lượng    : {rps:N0} reqs/sec (RPS)");
            TestContext.WriteLine("=========================================");

            // 7. Xác nhận test Pass/Fail
            Assert.That(completed, Is.True,
                $"Timeout! Chỉ nhận được {receivedCount}/{totalMessages} requests sau {timeoutSeconds} giây. Kiểm tra lại Deadlock hoặc độ trễ mạng.");
            Assert.That(receivedCount, Is.EqualTo(totalMessages), "Có hiện tượng rớt mạng, mất gói tin (Drop packet).");
        }

        // =========================================================================================
        // CASE: MULTI-ROUTER PERFORMANCE BENCHMARK (100K Requests - 5 Routers)
        // =========================================================================================
        [Test]
        public async Task Performance_MultiRouter_100k_Requests()
        {
            const int totalMessages = 100_000;
            const int totalPlayers = 5_000;
            const int routerCount = 5;

            // 1. Khởi tạo Service
            await using var service = new TransactionServiceClient(new[] { NatsUrl }, RedisUrl);
            await service.ConnectAsync();

            // 2. Khởi tạo 5 Routers
            var routers = new List<TransactionRouterClient>();
            for (int i = 1; i <= routerCount; i++)
            {
                var r = new TransactionRouterClient(new[] { NatsUrl }, id: i);
                await r.ConnectAsync();
                routers.Add(r);
            }

            var db = _redis.GetDatabase();
            int receivedCount = 0;
            var tcs = new TaskCompletionSource<bool>();

            // 3. Đăng ký Subscribe cho cả 5 Routers (dùng chung biến đếm an toàn luồng)
            var subs = new List<IDisposable>();
            foreach (var router in routers)
            {
                var sub = router.Subscribe<StringValue>("benchmark.multi", (msg, pId) =>
                {
                    var current = Interlocked.Increment(ref receivedCount);
                    if (current == totalMessages)
                    {
                        tcs.TrySetResult(true);
                    }
                });
                subs.Add(sub);
            }

            // 4. Set up Redis Route Map (Chia đều 5000 players cho 5 routers: ID từ 1 đến 5)
            var redisKeys = new KeyValuePair<RedisKey, RedisValue>[totalPlayers];
            for (int i = 0; i < totalPlayers; i++)
            {
                int assignedRouterId = (i % routerCount) + 1;
                redisKeys[i] = new KeyValuePair<RedisKey, RedisValue>($"pr:{i}", assignedRouterId);
            }

            await db.StringSetAsync(redisKeys);

            // 5. Warm-up (Khởi động mạng và đồng bộ Subscription của NATS)
            service.Publish(0, "benchmark.multi", new StringValue { Value = "Warmup" });
            await Task.Delay(1000); // Chờ NATS cluster đồng bộ đủ 5 channels
            Interlocked.Exchange(ref receivedCount, 0); // Reset biến đếm về 0

            // ==========================================
            // BẮT ĐẦU BENCHMARK 100K REQUESTS
            // ==========================================
            var sw = Stopwatch.StartNew();

            Parallel.For(0, totalMessages, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount },
                i =>
                {
                    long playerId = i % totalPlayers; // Tự động rải đều theo mảng Redis đã setup
                    service.Publish(playerId, "benchmark.multi", new StringValue { Value = "B" });
                });

            // Chờ tối đa 30 giây
            bool completed = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(30))) == tcs.Task;
            sw.Stop();

            // ==========================================
            // TÍNH TOÁN & XUẤT KẾT QUẢ
            // ==========================================
            double elapsedMs = sw.Elapsed.TotalMilliseconds;
            double rps = (totalMessages / elapsedMs) * 1000;

            TestContext.WriteLine("=====================================================");
            TestContext.WriteLine($"[Multi-Router Benchmark: {routerCount} Routers, 100K Reqs]");
            TestContext.WriteLine($" - Số lượng gửi   : {totalMessages:N0} reqs");
            TestContext.WriteLine($" - Số lượng nhận  : {receivedCount:N0} reqs");
            TestContext.WriteLine($" - Thời gian xử lý: {elapsedMs:N2} ms");
            TestContext.WriteLine($" - Thông lượng    : {rps:N0} reqs/sec (RPS)");
            TestContext.WriteLine("=====================================================");

            // Dọn dẹp thủ công các client
            foreach (var sub in subs) sub.Dispose();
            foreach (var r in routers) await r.DisposeAsync();

            // Xác nhận kết quả pass/fail
            Assert.That(completed, Is.True, $"Timeout! Chỉ nhận {receivedCount}/{totalMessages} requests sau 30 giây.");
            Assert.That(receivedCount, Is.EqualTo(totalMessages),
                "Có tình trạng rớt gói tin trong môi trường đa Router.");
        }

        // =========================================================================================
// CASE 7: ROUTING & DATA INTEGRITY (Service -> Multi-Router)
// Kiểm tra dữ liệu không bị hỏng khi gom Batch và đi đúng điểm đến
// =========================================================================================
        [Test]
        public async Task DataAndRouting_Correctness_ServiceToMultiRouter()
        {
            // 1. Khởi tạo 1 Service và 3 Router (ID: 10, 20, 30)
            await using var service = new TransactionServiceClient(new[] { NatsUrl }, RedisUrl);
            await using var router1 = new TransactionRouterClient(new[] { NatsUrl }, id: 10);
            await using var router2 = new TransactionRouterClient(new[] { NatsUrl }, id: 20);
            await using var router3 = new TransactionRouterClient(new[] { NatsUrl }, id: 30);

            await service.ConnectAsync();
            await router1.ConnectAsync();
            await router2.ConnectAsync();
            await router3.ConnectAsync();

            // Dùng ConcurrentBag để lưu lại an toàn dữ liệu nhận được trên nhiều luồng
            var r1Data = new ConcurrentBag<(long pId, string val)>();
            var r2Data = new ConcurrentBag<(long pId, string val)>();
            var r3Data = new ConcurrentBag<(long pId, string val)>();

            int messagesPerPlayer = 1000;
            int receivedCount = 0;
            var tcs = new TaskCompletionSource<bool>();

            // 2. Setup Route Map:
            // Router 10 quản lý Player 101, 102
            // Router 20 quản lý Player 201, 202
            // Router 30 quản lý Player 301, 302
            var routeMap = new Dictionary<long, int>
            {
                { 101, 10 }, { 102, 10 },
                { 201, 20 }, { 202, 20 },
                { 301, 30 }, { 302, 30 }
            };
            int totalExpectedMessages = messagesPerPlayer * routeMap.Count;

            // Hàm Local tiện ích để xử lý khi Router nhận được tin nhắn
            Action<StringValue, long> HandleMsg(ConcurrentBag<(long, string)> bag) => (msg, pId) =>
            {
                bag.Add((pId, msg.Value));
                if (Interlocked.Increment(ref receivedCount) == totalExpectedMessages)
                {
                    tcs.TrySetResult(true);
                }
            };

            // Các Router bắt đầu lắng nghe
            using var sub1 = router1.Subscribe<StringValue>("verify.data", HandleMsg(r1Data));
            using var sub2 = router2.Subscribe<StringValue>("verify.data", HandleMsg(r2Data));
            using var sub3 = router3.Subscribe<StringValue>("verify.data", HandleMsg(r3Data));

            // 3. Đẩy Routing rules lên Redis
            var db = _redis.GetDatabase();
            var redisKeys = routeMap.Select(kvp => new KeyValuePair<RedisKey, RedisValue>($"pr:{kvp.Key}", kvp.Value))
                .ToArray();
            await db.StringSetAsync(redisKeys);

            // Đợi 1 chút để NATS và Redis khởi tạo ổn định
            await Task.Delay(200);

            // 4. BẮT ĐẦU TEST: Service gửi dữ liệu ồ ạt
            // Trộn lẫn thứ tự gửi bằng Parallel để giả lập môi trường ThreadPool thực tế
            Parallel.For(0, messagesPerPlayer, i =>
            {
                foreach (var pId in routeMap.Keys)
                {
                    // Nội dung tin nhắn chính là "Chữ ký" để xác minh toàn vẹn dữ liệu
                    // Định dạng: "{PlayerId}-{Index}" (VD: "201-999")
                    string expectedPayload = $"{pId}-{i}";
                    service.Publish(pId, "verify.data", new StringValue { Value = expectedPayload });
                }
            });

            // 5. Đợi kết quả (Timeout 10 giây)
            bool completed = await Task.WhenAny(tcs.Task, Task.Delay(10000)) == tcs.Task;
            Assert.That(completed, Is.True,
                $"Timeout! Chỉ nhận được {receivedCount}/{totalExpectedMessages} messages.");

            // =========================================================
            // 6. KIỂM TRA TÍNH ĐÚNG ĐẮN CỦA ĐỊNH TUYẾN (ROUTING)
            // =========================================================
            Assert.That(r1Data.All(x => x.pId == 101 || x.pId == 102), Is.True,
                "LỖI ĐỊNH TUYẾN: Router 10 nhận nhầm data của player khác!");
            Assert.That(r2Data.All(x => x.pId == 201 || x.pId == 202), Is.True,
                "LỖI ĐỊNH TUYẾN: Router 20 nhận nhầm data của player khác!");
            Assert.That(r3Data.All(x => x.pId == 301 || x.pId == 302), Is.True,
                "LỖI ĐỊNH TUYẾN: Router 30 nhận nhầm data của player khác!");

            // =========================================================
            // 7. KIỂM TRA TÍNH TOÀN VẸN CỦA DỮ LIỆU (DATA INTEGRITY)
            // =========================================================
            // Đảm bảo số lượng nhận phân bổ đúng
            Assert.That(r1Data.Count, Is.EqualTo(messagesPerPlayer * 2));
            Assert.That(r2Data.Count, Is.EqualTo(messagesPerPlayer * 2));
            Assert.That(r3Data.Count, Is.EqualTo(messagesPerPlayer * 2));

            // Bóc tách kiểm tra nội dung chi tiết cho Player 101 xem Byte bị cắt có chuẩn không
            var player101Messages = r1Data.Where(x => x.pId == 101).Select(x => x.val).ToHashSet();
            Assert.That(player101Messages.Count, Is.EqualTo(messagesPerPlayer),
                "Bị mất gói tin (Drop packet) hoặc trùng lặp cho Player 101.");

            for (int i = 0; i < messagesPerPlayer; i++)
            {
                string targetSig = $"101-{i}";
                Assert.That(player101Messages.Contains(targetSig), Is.True,
                    $"LỖI DỮ LIỆU (Corruption): Không tìm thấy gói tin nguyên vẹn có nội dung '{targetSig}'. Khả năng cao do ArrayPool cấp phát đè bộ nhớ.");
            }
        }

        // =========================================================================================
// CASE 8: HEARTBEAT CLEANUP (Test luồng dọn dẹp người chơi AFK/Offline)
// =========================================================================================
        [Test]
        public async Task CleanupLoop_Should_Remove_Inactive_Players_After_10_Seconds()
        {
            await using var service = new TransactionServiceClient(new[] { NatsUrl }, RedisUrl);
            await service.ConnectAsync();

            var db = _redis.GetDatabase();
            long idlePlayerId = 9999;
            long activePlayerId = 8888;

            // 1. Fake data trực tiếp vào Redis với Timestamp cũ (giả lập đã offline 15 giây trước)
            long pastTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 15;
            await db.SortedSetAddAsync("active_players_ts", idlePlayerId, pastTime);
            await db.StringSetAsync($"pr:{idlePlayerId}", 1); // Mock route

            // 2. Fake data người chơi đang active (Timestamp hiện tại)
            long currentTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            await db.SortedSetAddAsync("active_players_ts", activePlayerId, currentTime);
            await db.StringSetAsync($"pr:{activePlayerId}", 1);

            // 3. Đợi 6 giây để CleanupLoopAsync của Service có cơ hội chạy (nó chạy mỗi 5s)
            await Task.Delay(6000);

            // 4. Kiểm tra kết quả
            var idleScore = await db.SortedSetScoreAsync("active_players_ts", idlePlayerId);
            var idleRoute = await db.StringGetAsync($"pr:{idlePlayerId}");

            var activeScore = await db.SortedSetScoreAsync("active_players_ts", activePlayerId);
            var activeRoute = await db.StringGetAsync($"pr:{activePlayerId}");

            // Đánh giá: Người chơi AFK phải bị xóa sạch khỏi SortedSet và xóa luôn Key định tuyến
            Assert.That(idleScore.HasValue, Is.False, "Lỗi: Không xóa Timestamp của người chơi AFK.");
            Assert.That(idleRoute.HasValue, Is.False, "Lỗi: Không xóa Route Key của người chơi AFK.");

            // Đánh giá: Người chơi Active phải được giữ lại an toàn
            Assert.That(activeScore.HasValue, Is.True, "Lỗi: Xóa nhầm người chơi đang hoạt động!");
            Assert.That(activeRoute.HasValue, Is.True, "Lỗi: Xóa nhầm Route của người chơi đang hoạt động!");
        }

        // =========================================================================================
// CASE 9: OVERSIZED SINGLE MESSAGE (1 Message > Max Batch Size)
// =========================================================================================
        [Test]
        public async Task Single_Message_Larger_Than_MaxBatchSize_Should_Not_Crash()
        {
            await using var service = new TransactionServiceClient(new[] { NatsUrl }, RedisUrl);
            await using var router = new TransactionRouterClient(new[] { NatsUrl }, id: 1);

            await service.ConnectAsync();
            await router.ConnectAsync();

            var db = _redis.GetDatabase();
            await db.StringSetAsync("pr:1", 1); // Định tuyến Player 1 -> Router 1

            var tcs = new TaskCompletionSource<bool>();
            int receivedLength = 0;

            using var sub = router.Subscribe<StringValue>("oversized.test", (msg, pId) =>
            {
                receivedLength = msg.Value.Length;
                tcs.TrySetResult(true);
            });

            // Tạo 1 string có kích thước 60KB (Max của bạn là 50KB)
            string oversizedString = new string('X', 60 * 1024);

            // Gửi đi
            service.Publish(1, "oversized.test", new StringValue { Value = oversizedString });

            bool received = await Task.WhenAny(tcs.Task, Task.Delay(3000)) == tcs.Task;

            Assert.That(received, Is.True, "Message siêu lớn bị kẹt hoặc làm crash luồng SendLoop.");
            Assert.That(receivedLength, Is.EqualTo(60 * 1024), "Dữ liệu bị cắt xén khi nhận!");
        }

        // =========================================================================================
// =========================================================================================
// CASE 10: UNSUBSCRIBE & MEMORY LEAK CHECK
// =========================================================================================
        [Test]
        public async Task Dispose_Subscription_Should_Stop_Receiving_And_Free_Memory()
        {
            // Khởi tạo cả 2 đầu để đi đúng luồng kiến trúc
            await using var service = new TransactionServiceClient(new[] { NatsUrl }, RedisUrl);
            await using var router = new TransactionRouterClient(new[] { NatsUrl }, id: 1);

            await service.ConnectAsync();
            await router.ConnectAsync();

            // Setup định tuyến: Player 1 -> Router 1
            var db = _redis.GetDatabase();
            await db.StringSetAsync("pr:1", 1);
            await Task.Delay(100); // Đợi Redis lưu xong

            int receiveCount = 0;

            // 1. Router đăng ký nhận sự kiện
            var subscription = router.Subscribe<StringValue>("unsubscribe.test",
                (msg, pId) => { Interlocked.Increment(ref receiveCount); });

            // Gửi thử 1 message từ Service (Service sẽ tự map ".1" vào subject)
            service.Publish(1, "unsubscribe.test", new StringValue { Value = "Msg1" });

            // Đợi Service flush batch và NATS truyền tải
            await Task.Delay(500);

            // Kiểm tra lần 1: Đảm bảo đã nhận được tin nhắn
            Assert.That(receiveCount, Is.EqualTo(1),
                "Không nhận được tin nhắn đầu tiên. Kiểm tra lại luồng Service -> Router.");

            // ==========================================
            // 2. GỌI DISPOSE ĐỂ HỦY ĐĂNG KÝ
            // ==========================================
            subscription.Dispose();

            // Đợi 1 chút để NATS Client kịp gỡ bỏ listener ngầm
            await Task.Delay(100);

            // 3. Tiếp tục gửi message sau khi đã hủy
            service.Publish(1, "unsubscribe.test", new StringValue { Value = "Msg2" });
            service.Publish(1, "unsubscribe.test", new StringValue { Value = "Msg3" });

            // Đợi xem có tin nhắn nào bị lọt xuống không
            await Task.Delay(500);

            // Đánh giá: Biến đếm không được tăng thêm, tức là Handler đã thực sự chết
            Assert.That(receiveCount, Is.EqualTo(1),
                "LỖI: Hủy Subscribe rồi nhưng vẫn tiếp tục nhận được tin nhắn ngầm!");
        }
    }
}