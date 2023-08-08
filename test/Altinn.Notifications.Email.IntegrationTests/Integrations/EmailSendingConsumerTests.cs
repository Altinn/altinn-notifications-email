using System.Text.Json;

using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Email.Integrations.Consumers;
using Altinn.Notifications.Email.IntegrationTests.Utils;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using Moq;

using Xunit;

namespace Altinn.Notifications.Email.IntegrationTests.Integrations;

public class EmailSendingConsumerTests : IDisposable
{
    private const string TestTopic = "email-sending";

    Mock<IEmailService> _emailServiceMock;

    public EmailSendingConsumerTests()
    {
        _emailServiceMock = new Mock<IEmailService>();
    }

    [Fact]
    public async Task ConsumeEmailTest_Successfull_deserialization_of_message_Service_called_once()
    {
        // Arrange
        await KafkaUtil.CreateTopicsAsync(TestTopic);

        Core.Models.Email email = 
            new(Guid.NewGuid(), "test", "body", "fromAddress", "toAddress", Core.Models.EmailContentType.Plain);

        await KafkaUtil.PostMessage(TestTopic, JsonSerializer.Serialize(email));

        var kafkaSettings = new KafkaSettings
        {
            BrokerAddress = "localhost:9092",
            EmailSendingConsumerSettings = new()
            {
                ConsumerGroupId = "email-sending-consumer",
                TopicName = TestTopic
            }
        };

        IServiceCollection services = new ServiceCollection()
            .AddLogging()
            .AddSingleton(kafkaSettings)
            .AddSingleton(_emailServiceMock.Object)
            .AddHostedService<EmailSendingConsumer>();

        var serviceProvider = services.BuildServiceProvider();

        using var sut = serviceProvider.GetService(typeof(IHostedService)) as EmailSendingConsumer;

        // Act
        await sut!.StartAsync(CancellationToken.None);
        await Task.Delay(10000);
        await sut.StopAsync(CancellationToken.None);

        // Assert
        _emailServiceMock.Verify(s => s.SendEmail(It.IsAny<Core.Models.Email>()), Times.Once);
    }

    public void Dispose()
    {
        KafkaUtil.DeleteTopicAsync(TestTopic).Wait();
    }
}
