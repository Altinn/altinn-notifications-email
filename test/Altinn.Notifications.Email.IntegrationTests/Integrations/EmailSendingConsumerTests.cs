using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Email.Integrations.Consumers;
using Altinn.Notifications.Email.IntegrationTests.Utils;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

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
    public async Task Test()
    {
        // Arrange
        await KafkaUtil.CreateTopicsAsync(TestTopic);
        await KafkaUtil.PostMessage(TestTopic, "test message");

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
        await Task.Delay(5000);
        await sut.StopAsync(CancellationToken.None);

        // Assert
        _emailServiceMock.Verify(s => s.SendEmail(It.IsAny<string>()), Times.Once);
    }

    public void Dispose()
    {
        KafkaUtil.DeleteTopicAsync(TestTopic).Wait();
    }
}
