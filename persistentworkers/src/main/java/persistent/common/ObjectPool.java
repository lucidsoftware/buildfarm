package persistent.common;

public interface ObjectPool<K, V> {
  V obtain(K key) throws Exception;

  void release(K key, V obj);

  /**
   * Invalidates an object, destroying it and freeing its slot in the pool. Use this instead of
   * {@link #release} when the object is in a bad state and should not be reused.
   */
  void invalidate(K key, V obj) throws Exception;
}
