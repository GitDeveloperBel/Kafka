using Avro;
using Avro.Specific;
using System.Text.Json;

namespace Shared;

public class Carrier : ISpecificRecord
{
    public string Data {  get; set; }

    public Schema Schema => Schema.Parse(File.ReadAllText("Carrier.avsc"));

    //public string Object { get; set; }

    private Carrier(string data)
    {
        Data = data;
    }

    public Carrier()
    {
        
    }

    public static Carrier Create<T>(T data)
    {
        return new(JsonSerializer.Serialize(data));
    }

    public object Get(int fieldPos)
    {
        switch (fieldPos)
        {
            case 0: return Data;
            default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
        }
    }

    public void Put(int fieldPos, object fieldValue)
    {
        switch (fieldPos)
        {
            case 0: Data = (string)fieldValue;break;
            default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
        }
    }
}
