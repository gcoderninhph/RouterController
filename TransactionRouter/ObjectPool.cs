namespace Gcoder.Poll;

public class ObjectPool<T> where T : new()
{
    private readonly Queue<T> _pool;
    private readonly Action<T> _resetAction;

    public ObjectPool(Action<T> resetAction, int lengthDefault = 10)
    {
        _resetAction = resetAction ?? throw new ArgumentNullException(nameof(resetAction));
        _pool = new Queue<T>(lengthDefault);

        for (int i = 0; i < lengthDefault; i++)
        {
            _pool.Enqueue(new T());
        }
    }

    public T Rent()
    {
        T item = _pool.Count > 0 ? _pool.Dequeue() : new T();
        _resetAction(item);
        return item;
    }

    public void Return(T item)
    {
        if (item == null) throw new ArgumentNullException(nameof(item));
        _resetAction(item);
        _pool.Enqueue(item);
    }
}