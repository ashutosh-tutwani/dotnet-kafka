// See https://aka.ms/new-console-template for more information

using Avro.Specific;
using Avro;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Schema = Avro.Schema;

public class KafkaProducer
{
    private readonly SchemaRegistryConfig schemaRegistryConfig;
    private ProducerConfig config;

    public KafkaProducer()
    {
        schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "http://localhost:8081" // Schema Registry URL
        };
        config = KafkaProducerConfig.GetConfig();
    }

    public void ProduceMessage()
    {
        using var producer = new ProducerBuilder<Null, string>(config).Build();
        string topic = "test"; // Replace with your topic name
        string message = "Hello, Kafka!";
        var deliveryReport = producer.ProduceAsync(topic, new Message<Null, string> { Value = message }).Result;

        Console.WriteLine($"Produced message to {deliveryReport.Topic} partition {deliveryReport.Partition} @ offset {deliveryReport.Offset}");
    }

    public void ProduceMultipleMessages(int number)
    {
        using var producer = new ProducerBuilder<string, string>(config).Build();
        string topic = "test"; // Replace with your topic name    

        for (int i = 1; i < 10000; i++)
        {
            string message = $"Hello, Kafka! {i}";
            var deliveryReport = producer.ProduceAsync(topic,
                new Message<string, string>
                {
                    Value = message,
                    //Key = "key12"
                }).Result;

            Console.WriteLine($"Produced message to {deliveryReport.Topic} partition {deliveryReport.Partition} @ offset {deliveryReport.Offset}");
        }
    }


    public void ProduceMultipleAvroMessages(int number)
    {
        using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
        {
            var producerBuilder = new ProducerBuilder<string, User>(config)
                 .SetValueSerializer(new AvroSerializer<User>(schemaRegistry));

            using (var producer = producerBuilder.Build())
            {
                //for (int i = 1; i < 10000; i++)
                //{
                    var deliveryReport = producer.ProduceAsync("avro-topic", new Message<string, User>
                    {
                        Key = "key1",
                        Value = new User { Name = Guid.NewGuid().ToString().Substring(0,4), Age = new Random().Next(100,200), Male= new Random().Next(100, 200) }
                    }).Result;

                    Console.WriteLine($"Produced message to {deliveryReport.Topic} partition {deliveryReport.Partition} @ offset {deliveryReport.Offset}");
                //}
            }
        }
    }
}


public class User : ISpecificRecord
{
    public User() { }

    public User(string name, int age)
    {
        Name = name;
        Age = age;
    }

    public string Name { get; set; }
    public int Age { get; set; }
    public int Male { get; set; }

    Schema ISpecificRecord.Schema => Schema.Parse(@"{
        ""type"":""record"",
        ""name"":""User"",
        ""fields"":[
            {""name"":""Name"",""type"":""string""},
            {""name"":""Age"",""type"":""int""},
            {""name"":""Male"",""type"":""int""}
        ]
    }");

    public virtual object Get(int fieldPos) => fieldPos switch
    {
        0 => Name,
        1 => Age,
        2 => Male,
        _ => throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()")
    };

    public virtual void Put(int fieldPos, object fieldValue)
    {
        switch (fieldPos)
        {
            case 0:
                Name = (string)fieldValue;
                break;
            case 1:
                Age = (int)fieldValue;
                break;
            default:
                throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
        }
    }
}
