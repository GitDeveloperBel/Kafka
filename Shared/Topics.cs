namespace Shared;

public static class Topics
{
#if !DEBUGPART
    public const string TOPIC_ORDER = "orderPlaced"; 
#else
    public const string TOPIC_ORDER = "orderPlaced2";
#endif
    public const string TOPIC_DISH = "dishPlaced"; 
}
