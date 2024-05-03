using System.Text.Json;

namespace Kafka_Producer;

internal class Carrier
{
    public string Data {  get; set; }
    //public string Object { get; set; }

    private Carrier(string data)
    {
        Data = data;
    }

    public static Carrier Create<T>(T data)
    {
        return new(JsonSerializer.Serialize(data));
    }
}
