
using Avro;
using Avro.Specific;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Schema = Avro.Schema;

public class KafkaConsumer
{
    private readonly SchemaRegistryConfig schemaRegistryConfig;
    private readonly ConsumerConfig config;

    public KafkaConsumer()
    {
        schemaRegistryConfig = new SchemaRegistryConfig
        {
            Url = "http://localhost:8081" // Schema Registry URL
        };
        config = KafkaConsumerConfig.GetConfig();
    }

    public void ConsumeMessages()
    {
        string topic = "test"; // Replace with your topic name
        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

        consumer.Subscribe(topic);

        while (true)
        {
            try
            {
                var consumeResult = consumer.Consume();
                Console.WriteLine($"Received message: {consumeResult.Message.Value}");

                // Process the message here
                consumer.Commit(consumeResult);
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Error consuming message: {e.Error.Reason}");
            }
        }
    }

    public void ConsumeMessagesFromSInglePartition()
    {
        string topic = "test"; // Replace with your topic name

        var Config = KafkaConsumerConfig.GetConfig();
        Config.GroupId = "consumer-grp-2";
        using var consumer = new ConsumerBuilder<Ignore, string>(Config).Build();

        consumer.Assign(new TopicPartition(topic, 1));

        while (true)
        {
            try
            {
                var consumeResult = consumer.Consume();
                Console.WriteLine($"Received message: {consumeResult.Message.Value}");

                // Process the message here
                consumer.Commit(consumeResult);
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Error consuming message: {e.Error.Reason}");
            }
        }
    }

    public void ConsumeMessagesFromMultiplePartition()
    {
        string topic = "test"; // Replace with your topic name

        var Config = KafkaConsumerConfig.GetConfig();
        //Config.GroupId = "consumer-grp-2";
        using var consumer = new ConsumerBuilder<Ignore, string>(Config).Build();

        //consumer.Assign(new TopicPartition(topic, 1));
        consumer.Subscribe(topic);

        while (true)
        {
            try
            {
                var consumeResult = consumer.Consume();
                Console.WriteLine($"Received message: {consumeResult.Message.Value},  partition {consumeResult.Partition} and offset {consumeResult.TopicPartitionOffset} ");
                Thread.Sleep(10000);
                // Process the message here
                consumer.Commit(consumeResult);
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Error consuming message: {e.Error.Reason}");
            }
        }
    }


    public void ConsumeAvroMessages()
    {
        string topic = "avro-topic"; // Replace with your topic name

        using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
        {
            var consumerBuilder = new ConsumerBuilder<string, User>(config)
                   .SetValueDeserializer(new AvroDeserializer<User>(schemaRegistry).AsSyncOverAsync());

            using (var consumer = consumerBuilder.Build())
            {
                consumer.Subscribe(topic);

                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume();
                        Console.WriteLine($"Received message: {consumeResult.Message.Value},  partition {consumeResult.Partition} and offset {consumeResult.TopicPartitionOffset} ");

                        // Process the message here
                        consumer.Commit(consumeResult);
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occurred: {e.Error.Reason}");
                    }
                }
            }
        }     
    }
}

public class User : ISpecificRecord
{
    public User() { }

    public User(string name, int age, int male)
    {
        Name = name;
        Age = age;
        Male = male;
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
            {""name"":""Male"",""type"":""int"", ""default"": 0}
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
            case 2:
                Male = (int)fieldValue;
                break;
            default:
                throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
        }
    }
}