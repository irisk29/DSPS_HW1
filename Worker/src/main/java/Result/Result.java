package Result;

public interface Result<T> {
    public boolean getTag();
    public String getMessage();
    public T getValue();
}
