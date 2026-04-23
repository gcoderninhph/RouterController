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

        private const string ClientName = "client";
        private const string ServerName = "server";
        private const string SortSetTimeOutName = $"{ServerName}_timeout";

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
            var service = new TransactionServer(NatsUrl, RedisUrl, ServerName, ClientName);
            var router = new TransactionClient(NatsUrl, ClientName, ServerName);


            var tcsServiceReceive = new TaskCompletionSource<bool>();
            var tcsRouterReceive = new TaskCompletionSource<bool>();
            long testPlayerId = 1001;

            // 1. Service lắng nghe "auth.login" từ Router (Đồng thời học được Route vào Redis)
            service.Subscribe<StringValue>("auth.login", (msg, pId) =>
            {
                Assert.That(pId, Is.EqualTo(testPlayerId));
                Assert.That(msg.Value, Is.EqualTo("HelloService"));
                tcsServiceReceive.TrySetResult(true);
            });

            // 2. Router lắng nghe "game.events" từ Service
            router.Subscribe<StringValue>("game.events", (msg, pId) =>
            {
                Assert.That(pId, Is.EqualTo(testPlayerId));
                Assert.That(msg.Value, Is.EqualTo("HelloRouter"));
                tcsRouterReceive.TrySetResult(true);
            });

            // Hành động: Router gửi Service -> Service học Route
            router.Publish("auth.login", testPlayerId, new StringValue { Value = "HelloService" });

            // Đợi Service nhận được
            await Task.WhenAny(tcsServiceReceive.Task, Task.Delay(2000));
            Assert.That(tcsServiceReceive.Task.IsCompletedSuccessfully, Is.True, "Service không nhận được message.");

            // Đợi 1 chút để Service đẩy _redisDb.StringSetAsync (học route) xong ở background
            await Task.Delay(200);

            // Hành động: Service phản hồi lại Router
            service.Publish("game.events", testPlayerId, new StringValue { Value = "HelloRouter" });

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
            var service = new TransactionServer(NatsUrl, RedisUrl, ServerName, ClientName);
            var router = new TransactionClient(NatsUrl, ClientName, ServerName);

            const int totalMessages = 100_000;
            const int totalPlayers = 5000;

            var receivedCount = 0;
            var countdown = new CountdownEvent(totalMessages);

            // Subscriber: Router lắng nghe
            router.Subscribe<StringValue>("stress.test", (msg, pId) =>
            {
                Interlocked.Increment(ref receivedCount);
                countdown.Signal();
            });

            // Fake Route trong Redis trước để Service biết đường gửi (bypass bước Router gửi lên Service)
            var db = _redis.GetDatabase();
            var redisKeys = new KeyValuePair<RedisKey, RedisValue>[totalPlayers];
            for (int i = 0; i < totalPlayers; i++)
            {
                redisKeys[i] = new KeyValuePair<RedisKey, RedisValue>($"{ServerName}_pr:{i}", router.Id);
            }

            await db.StringSetAsync(redisKeys);

            // Bắn phá 100,000 tin nhắn bằng đa luồng
            var sw = Stopwatch.StartNew();
            Parallel.For(0, totalMessages, i =>
            {
                long playerId = i % totalPlayers; // Chia đều cho 5000 players
                service.Publish("stress.test", playerId, new StringValue { Value = $"Msg_{i}" });
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
            using var service = new TransactionServer(NatsUrl, RedisUrl, ServerName, ClientName);
            using var router = new TransactionClient(NatsUrl, ClientName, ServerName);


            int received = 0;
            var tcs = new TaskCompletionSource<bool>();

            // FIX: Dùng Service để Subscribe. 
            // Theo logic, Service sẽ lắng nghe đúng kênh gốc ("large.payload")
            service.Subscribe<StringValue>("large.payload", (msg, pId) =>
            {
                Interlocked.Increment(ref received);
                if (received == 2) tcs.TrySetResult(true);
            });

            // MaxBatchPayloadSize = 50 * 1024 (50KB)
            // Tạo 1 string khoảng 30KB
            string hugeString = new string('A', 30 * 1024);

            // Gửi 2 message x 30KB = 60KB. Vượt 50KB -> Sẽ ép Trigger SendCurrentBatch ngay ở vòng lặp
            router.Publish("large.payload", 1, new StringValue { Value = hugeString });
            router.Publish("large.payload", 2, new StringValue { Value = hugeString });

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
            using var service = new TransactionServer(NatsUrl, RedisUrl, ServerName, ClientName);
            using var router1 = new TransactionClient(NatsUrl, ClientName, ServerName);
            using var router2 = new TransactionClient(NatsUrl, ClientName, ServerName);


            var r1Received = new ConcurrentBag<long>();
            var r2Received = new ConcurrentBag<long>();

            router1.Subscribe<StringValue>("multi.route", (msg, pId) => r1Received.Add(pId));
            router2.Subscribe<StringValue>("multi.route", (msg, pId) => r2Received.Add(pId));

            // Set Route thủ công
            var db = _redis.GetDatabase();
            await db.StringSetAsync($"{ServerName}_pr:100", router1.Id); // Player 100 -> Router 10
            await db.StringSetAsync($"{ServerName}_pr:200", router2.Id); // Player 200 -> Router 20

            // Service gửi
            service.Publish("multi.route", 100, new StringValue { Value = "A" });
            service.Publish("multi.route", 200, new StringValue { Value = "B" });
            service.Publish("multi.route", 100, new StringValue { Value = "C" });

            await Task.Delay(500); // Chờ NATS chuyển phát

            Assert.That(r1Received.Count, Is.EqualTo(2), "Router 1 phải nhận 2 messages của player 100");
            Assert.That(r1Received.All(x => x == 100), Is.True);

            Assert.That(r2Received.Count, Is.EqualTo(1), "Router 2 phải nhận 1 messages của player 200");
            Assert.That(r2Received.All(x => x == 200), Is.True);
        }

        // =========================================================================================
        // CASE 6: HEARTBEAT REDIS (Đảm bảo Service update active players vào SortedSet)
        // =========================================================================================
        [Test]
        public async Task Heartbeat_Should_Update_To_Redis_SortedSet()
        {
            var service = new TransactionServer(NatsUrl, RedisUrl, ServerName, ClientName);
            using var router = new TransactionClient(NatsUrl, ClientName, ServerName);

            long testPlayerId = 555;


            // Gửi 1 gói để trigger UpdateHeartbeatsAsync
            service.Subscribe<StringValue>("ping", (msg, playerId) => { });
            router.Publish("ping", testPlayerId, new StringValue { Value = "ping" });

            // Background thread đẩy redis, cần đợi tí
            await Task.Delay(500);

            var score = await _redis.GetDatabase().SortedSetScoreAsync(SortSetTimeOutName, testPlayerId);
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
            var service = new TransactionServer(NatsUrl, RedisUrl, ServerName, ClientName);
            var router = new TransactionClient(NatsUrl, ClientName, ServerName);


            var db = _redis.GetDatabase();
            int receivedCount = 0;
            var countdown = new CountdownEvent(totalMessages);
            var tcs = new TaskCompletionSource<bool>();

            // 2. Router đăng ký nhận sự kiện từ Service
            router.Subscribe<StringValue>("benchmark.events", (msg, pId) =>
            {
                var current = Interlocked.Increment(ref receivedCount);
                if (current == totalMessages)
                {
                    tcs.TrySetResult(true);
                }
            });

            // 3. Khởi tạo dữ liệu giả lập trên Redis: Phân bổ đều Player vào Router
            // Làm bước này để Service có thể lấy Route ngay lập tức mà không bị drop gói tin
            var redisKeys = new KeyValuePair<RedisKey, RedisValue>[totalPlayers];
            for (int i = 0; i < totalPlayers; i++)
            {
                redisKeys[i] = new KeyValuePair<RedisKey, RedisValue>($"{ServerName}_pr:{i}", router.Id);
            }

            await db.StringSetAsync(redisKeys);

            // Warm-up NATS connection (gửi mồi 1 tin nhắn để thiết lập đường truyền mạng)
            service.Publish("benchmark.events", 0, new StringValue { Value = "Warmup" });
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
                    service.Publish("benchmark.events", playerId, new StringValue { Value = "B" });
                });

            // 5. Đợi kết quả (Timeout linh hoạt: 5 giây cho 1k, 10 giây cho 10k, 30 giây cho 100k)
            int timeoutSeconds = totalMessages > 50000 ? 30 : 10;
            bool completed = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(timeoutSeconds))) == tcs.Task;

            sw.Stop();

            // 6. In Report ra Console / Test Output
            double elapsedMs = sw.Elapsed.TotalMilliseconds;
            double rps = (totalMessages / elapsedMs) * 1000;

            TestContext.Out.WriteLine("=========================================");
            TestContext.Out.WriteLine($"[{testName}] Báo cáo Hiệu suất:");
            TestContext.Out.WriteLine($" - Số lượng gửi   : {totalMessages:N0} reqs");
            TestContext.Out.WriteLine($" - Số lượng nhận  : {receivedCount:N0} reqs");
            TestContext.Out.WriteLine($" - Thời gian xử lý: {elapsedMs:N2} ms");
            TestContext.Out.WriteLine($" - Thông lượng    : {rps:N0} reqs/sec (RPS)");
            TestContext.Out.WriteLine("=========================================");

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
            var service = new TransactionServer(NatsUrl, RedisUrl, ServerName, ClientName);


            // 2. Khởi tạo 5 Routers
            var routers = new List<TransactionClient>();
            for (int i = 1; i <= routerCount; i++)
            {
                var r = new TransactionClient(NatsUrl, ClientName, ServerName);
                routers.Add(r);
            }

            var db = _redis.GetDatabase();
            int receivedCount = 0;
            var tcs = new TaskCompletionSource<bool>();


            foreach (var router in routers)
            {
                router.Subscribe<StringValue>("benchmark.multi", (msg, pId) =>
                {
                    var current = Interlocked.Increment(ref receivedCount);
                    if (current == totalMessages)
                    {
                        tcs.TrySetResult(true);
                    }
                });
            }

            // 4. Set up Redis Route Map (Chia đều 5000 players cho 5 routers: ID từ 1 đến 5)
            var redisKeys = new KeyValuePair<RedisKey, RedisValue>[totalPlayers];
            for (int i = 0; i < totalPlayers; i++)
            {
                var randomIndex = new Random().Next(0, routers.Count); // Chọn ngẫu nhiên 1 router trong 5 router
                redisKeys[i] = new KeyValuePair<RedisKey, RedisValue>($"{ServerName}_pr:{i}", routers[randomIndex].Id);
            }

            await db.StringSetAsync(redisKeys);

            // 5. Warm-up (Khởi động mạng và đồng bộ Subscription của NATS)
            service.Publish("benchmark.multi", 0, new StringValue { Value = "Warmup" });
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
                    service.Publish("benchmark.multi", playerId, new StringValue { Value = "B" });
                });

            // Chờ tối đa 30 giây
            bool completed = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(30))) == tcs.Task;
            sw.Stop();

            // ==========================================
            // TÍNH TOÁN & XUẤT KẾT QUẢ
            // ==========================================
            double elapsedMs = sw.Elapsed.TotalMilliseconds;
            double rps = (totalMessages / elapsedMs) * 1000;

            TestContext.Out.WriteLine("=====================================================");
            TestContext.Out.WriteLine($"[Multi-Router Benchmark: {routerCount} Routers, 100K Reqs]");
            TestContext.Out.WriteLine($" - Số lượng gửi   : {totalMessages:N0} reqs");
            TestContext.Out.WriteLine($" - Số lượng nhận  : {receivedCount:N0} reqs");
            TestContext.Out.WriteLine($" - Thời gian xử lý: {elapsedMs:N2} ms");
            TestContext.Out.WriteLine($" - Thông lượng    : {rps:N0} reqs/sec (RPS)");
            TestContext.Out.WriteLine("=====================================================");

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
            var service = new TransactionServer(NatsUrl, RedisUrl, ServerName, ClientName);
            var router1 = new TransactionClient(NatsUrl, ClientName, ServerName);
            var router2 = new TransactionClient(NatsUrl, ClientName, ServerName);
            var router3 = new TransactionClient(NatsUrl, ClientName, ServerName);


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
            var routeMap = new Dictionary<long, string>
            {
                { 101, router1.Id }, { 102, router1.Id },
                { 201, router2.Id }, { 202, router2.Id },
                { 301, router3.Id }, { 302, router3.Id }
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
            router1.Subscribe<StringValue>("verify.data", HandleMsg(r1Data));
            router2.Subscribe<StringValue>("verify.data", HandleMsg(r2Data));
            router3.Subscribe<StringValue>("verify.data", HandleMsg(r3Data));

            // 3. Đẩy Routing rules lên Redis
            var db = _redis.GetDatabase();
            var redisKeys = routeMap.Select(kvp =>
                    new KeyValuePair<RedisKey, RedisValue>($"{ServerName}_pr:{kvp.Key}", kvp.Value))
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
                    service.Publish("verify.data", pId, new StringValue { Value = expectedPayload });
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
            var service = new TransactionServer(NatsUrl, RedisUrl, ServerName, ClientName);
            
            var db = _redis.GetDatabase();
            long idlePlayerId = 9999;
            long activePlayerId = 8888;

            // 1. Fake data trực tiếp vào Redis với Timestamp cũ (giả lập đã offline 15 giây trước)
            long pastTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds() - 15;
            await db.SortedSetAddAsync(SortSetTimeOutName, idlePlayerId, pastTime);
            await db.StringSetAsync($"{ServerName}_pr:{idlePlayerId}", 1); // Mock route

            // 2. Fake data người chơi đang active (Timestamp hiện tại)
            long currentTime = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            await db.SortedSetAddAsync(SortSetTimeOutName, activePlayerId, currentTime);
            await db.StringSetAsync($"{ServerName}_pr:{activePlayerId}", 1);

            // 3. Đợi 6 giây để CleanupLoopAsync của Service có cơ hội chạy (nó chạy mỗi 5s)
            await Task.Delay(6000);

            // 4. Kiểm tra kết quả
            var idleScore = await db.SortedSetScoreAsync(SortSetTimeOutName, idlePlayerId);
            var idleRoute = await db.StringGetAsync($"{ServerName}_pr:{idlePlayerId}");

            var activeScore = await db.SortedSetScoreAsync(SortSetTimeOutName, activePlayerId);
            var activeRoute = await db.StringGetAsync($"{ServerName}_pr:{activePlayerId}");

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
            var service = new TransactionServer(NatsUrl, RedisUrl, ServerName, ClientName);
            var router = new TransactionClient(NatsUrl, ClientName, ServerName);


            var db = _redis.GetDatabase();
            await db.StringSetAsync($"{ServerName}_pr:1", router.Id); // Định tuyến Player 1 -> Router 1

            var tcs = new TaskCompletionSource<bool>();
            int receivedLength = 0;

            router.Subscribe<StringValue>("oversized.test", (msg, pId) =>
            {
                receivedLength = msg.Value.Length;
                tcs.TrySetResult(true);
            });

            // Tạo 1 string có kích thước 60KB (Max của bạn là 50KB)
            string oversizedString = new string('X', 60 * 1024);

            // Gửi đi
            service.Publish("oversized.test", 1, new StringValue { Value = oversizedString });

            bool received = await Task.WhenAny(tcs.Task, Task.Delay(3000)) == tcs.Task;

            Assert.That(received, Is.True, "Message siêu lớn bị kẹt hoặc làm crash luồng SendLoop.");
            Assert.That(receivedLength, Is.EqualTo(60 * 1024), "Dữ liệu bị cắt xén khi nhận!");
        }
    }
}