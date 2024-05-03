using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System.Text.Json;

namespace Kafka_Consumer;

internal class ConsumerHandler<T>
{
    private readonly string _topic;
    private readonly SchemaRegistryConfig _schemaRegistryConfig;
    private readonly ConsumerConfig _consumerConfig;
    private readonly AvroDeserializerConfig _serialiserConfig;

    private readonly Action<T> _action;
    public ConsumerHandler(string topic, Action<T> handle)
    {
        _topic = topic;
        _schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "localhost:8081",
        };
        _consumerConfig = new ConsumerConfig
        {
            GroupId = Guid.NewGuid().ToString(),
            BootstrapServers = "locahlhost:9092",
            AutoOffsetReset = AutoOffsetReset.Latest
        };
        _serialiserConfig = new();
        _action = handle;
    }

    public void Consume(CancellationTokenSource cts)
    {
        using var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
        using var consumer = new ConsumerBuilder<Null, Carrier>(_consumerConfig).SetValueDeserializer(new AvroDeserializer<Carrier>(schemaRegistry).AsSyncOverAsync()).Build();
        consumer.Subscribe(_topic);
        while (true)
        {
            var cr = consumer.Consume(cts.Token);
            var carrier = cr.Message.Value; // TODO: try catch
            _action.Invoke(JsonSerializer.Deserialize<T>(carrier.Data)!);
        }
    }
}
