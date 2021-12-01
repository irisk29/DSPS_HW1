package Result;

public class Fail<T> implements Result<T> {
    private String message;
    private T value;

    public Fail(String message, T value)
    {
        this.message = message;
        this.value = value;
    }

    public boolean getTag()
    {
        return false;
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
