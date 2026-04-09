using System.Collections.Concurrent;

namespace Gcoder.Poll;

// Vẫn giữ nguyên where T : new() như ban đầu của bạn
public class ObjectPool<T> where T : new()
{
    // THAY ĐỔI: Dùng ConcurrentBag thay cho Queue để an toàn khi chạy đa luồng (Zero GC)
    private readonly ConcurrentBag<T> _pool;
    private readonly Action<T> _resetAction;

    // GIỮ NGUYÊN 100% tham số truyền vào: (Action<T> resetAction, int lengthDefault = 10)
    public ObjectPool(Action<T> resetAction, int lengthDefault = 10)
    {
        _resetAction = resetAction ?? throw new ArgumentNullException(nameof(resetAction));
        _pool = new ConcurrentBag<T>();

        for (int i = 0; i < lengthDefault; i++)
        {
            _pool.Add(new T());
        }
    }

    // GIỮ NGUYÊN cách gọi Rent()
    public T Rent()
    {
        // TryTake của ConcurrentBag không tạo ra rác (Zero Allocation)
        if (_pool.TryTake(out T item))
        {
            return item;
        }
        
        // Nếu pool cạn, tạo mới (chỉ tốn GC lúc hệ thống bị quá tải, sau đó sẽ ổn định)
        return new T();
    }

    // GIỮ NGUYÊN cách gọi Return()
    public void Return(T item)
    {
        if (item == null) throw new ArgumentNullException(nameof(item));
        
        // LƯU Ý QUAN TRỌNG NHẤT: Hàm reset BẮT BUỘC phải gọi ở đây trước khi đưa vào Pool.
        // Điều này giúp dọn sạch List/Array bên trong Object, ngăn chặn Memory Leak.
        _resetAction(item);
        
        _pool.Add(item);
    }
}