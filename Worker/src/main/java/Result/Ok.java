package Result;

public class Ok<T> implements Result<T> {
    private String message;
    private T value;

    public Ok(String message, T value)
    {
        this.message = message;
        this.value = value;
    }

    public boolean getTag()
    {
        return true;
    }

    public String getMessage()
    {
        return this.message;
    }

    public T getValue()
    {
        return this.value;
    }
}
