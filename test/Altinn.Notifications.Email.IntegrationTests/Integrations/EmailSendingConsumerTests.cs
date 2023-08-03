using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Email.Integrations.Consumers;
using Altinn.Notifications.Email.IntegrationTests.Utils;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;
using System.Text.Json;
using Xunit;

namespace Altinn.Notifications.Email.IntegrationTests.Integrations;

public class EmailSendingConsumerTests : IDisposable
{
    private const string TestTopic = "email-sending";

    ILogger<EmailSendingConsumer> _logger;

    Mock<IEmailService> _emailServiceMock;

    public EmailSendingConsumerTests()
    {
        _logger = NullLoggerFactory.Instance.CreateLogger<EmailSendingConsumer>();
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
            EmailSendingConsumerSettings = new() {
                ConsumerGroupId = "email-sending-consumer",
                TopicName = TestTopic 
            }
        };
        var sut = new EmailSendingConsumer(kafkaSettings, _emailServiceMock.Object, _logger);

        // Act
        await sut.StartAsync(CancellationToken.None);
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
