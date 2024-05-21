
using Confluent.Kafka;

public static class KafkaConsumerConfig
{
    public static ConsumerConfig GetConfig()
    {
        return new ConsumerConfig
        {
            BootstrapServers = "localhost:9092", // Replace with your Kafka broker address
            GroupId = "KafkaExampleConsumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
           // EnableAutoCommit = false,
        };
    }
}