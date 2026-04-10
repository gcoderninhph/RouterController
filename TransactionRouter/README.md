HƯỚNG DẪN SỬ DỤNG TRANSACTION CLIENT

1.  TRANSACTION ROUTER CLIENT (Dành cho Gateway/Router giữ kết nối với Client)

<!-- end list -->

- Khởi tạo: var router = new TransactionRouterClient(new[] { "nats://..." }, routerId);
- Kết nối: await router.ConnectAsync();
- Gửi tin (Publish): router.Publish(playerId, "subject\_name", protobufMessage);
- Nhận tin (Subscribe):
  var sub = router.Subscribe\<MyProtoMsg\>("subject\_name", (msg, playerId) =\> { /\* xử lý \*/ }, resetAction: msg =\> msg.Clear());
  (Có hỗ trợ handler dạng Async trả về Task).
- Hủy nhận định kỳ: sub.Dispose();
- Dọn dẹp tài nguyên (khi tắt app): await router.DisposeAsync();

<!-- end list -->

2.  TRANSACTION SERVICE CLIENT (Dành cho Backend/Microservices)

<!-- end list -->

- Khởi tạo: var service = new TransactionServiceClient(new[] { "nats://..." }, "redis\_connection\_string");
- Kết nối: await service.ConnectAsync();
- Gửi tin (Publish): service.Publish(playerId, "subject\_name", protobufMessage);
  (Ghi chú: Client sẽ tự động định tuyến gói tin đến đúng Router đang giữ người chơi đó thông qua Redis).
- Nhận tin (Subscribe):
  var sub = service.Subscribe\<MyProtoMsg\>("subject\_name", async (msg, playerId) =\> { /\* xử lý \*/ }, group: "queue\_group\_name", resetAction: msg =\> msg.Clear());
  (Ghi chú: Cung cấp thêm tham số `group` để cấu hình Queue Group load-balancing giữa các instance).
- Dọn dẹp tài nguyên (khi tắt app): await service.DisposeAsync();

<!-- end list -->

3.  CÁC LƯU Ý QUAN TRỌNG BẮT BUỘC

<!-- end list -->

- Kiểu dữ liệu: Mọi message gửi/nhận bắt buộc phải là đối tượng Protobuf (implement Google.Protobuf.IMessage).
- Tham số `resetAction` (trong hàm Subscribe): Dù là tùy chọn nhưng RẤT KHUYẾN NGHỊ sử dụng. Bạn cần truyền vào hàm clear các field của message (ví dụ: msg =\> msg.Clear()) để hệ thống reset object trước khi đưa lại vào ObjectPool, giúp tối ưu Zero-Allocation.
- Vòng đời: Bắt buộc phải gọi await ConnectAsync() trước khi thao tác và gọi await DisposeAsync() khi tắt dịch vụ để giải phóng các background task, NATS và Redis.