// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;

public static class KafkaProducerConfig
{
    public static ProducerConfig GetConfig()
    {
        return new ProducerConfig
        {
            BootstrapServers = "localhost:9092", // Replace with your Kafka broker address
            ClientId = "KafkaExampleProducer",
            Acks = Acks.All
        };
    }
}