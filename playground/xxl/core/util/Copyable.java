package xxl.core.util;

/* Bounded parameter type since a class that implements this interface
 * should only be able to create copies of the same type (or a subtype).
 */
public interface Copyable<T extends Copyable<T>>
{
  public T copy();
}