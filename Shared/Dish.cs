using Avro;
using Avro.Specific;

namespace Shared;

public class Dish : ISpecificRecord
{
    public Guid Id { get; set; }
    public int AmountOfEggs { get; set; }
    public int AmountOfBread { get; set; }

    public Schema Schema => Schema.Parse(File.ReadAllText("Dish.avsc"));

    public Dish()
    {
        
    }

    public Dish(Guid id, int amountOfEggs, int amountOfBread)
    {
        Id = id;
        AmountOfEggs = amountOfEggs;
        AmountOfBread = amountOfBread;        
    }

    public object Get(int fieldPos)
    {
        switch (fieldPos)
        {
            case 0: return Id.ToString();
            case 1: return AmountOfEggs;
            case 2: return AmountOfBread;
            default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
        }
    }

    public void Put(int fieldPos, object fieldValue)
    {
        switch (fieldPos)
        {
            case 0: Id = Guid.Parse((string)fieldValue); break;
            case 1: AmountOfEggs = (int)fieldValue; break;
            case 2: AmountOfBread = (int)fieldValue; break;
            default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
        }
    }
}
