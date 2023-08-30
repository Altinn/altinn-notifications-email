using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Core.Dependencies;
using Altinn.Notifications.Email.Core.Models;
using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Email.Integrations.Consumers;
using Altinn.Notifications.Email.Integrations.Producers;
using Altinn.Notifications.Email.IntegrationTests.Utils;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using NSubstitute;

using System.Text.Json;

using Xunit;

namespace Altinn.Notifications.Email.IntegrationTests.Integrations;

public sealed class EmailSendingConsumerTests : IAsyncLifetime
{
    private readonly string EmailSendingConsumerTopic = Guid.NewGuid().ToString();
    private readonly string EmailSendingAcceptedProducerTopic = Guid.NewGuid().ToString();

    private readonly IEmailService _emailServiceMock;

    private readonly ServiceProvider _serviceProvider;

    public EmailSendingConsumerTests()
    {
        _emailServiceMock = Substitute.For<IEmailService>();

        var kafkaSettings = new KafkaSettings
        {
            BrokerAddress = "localhost:9092",
            Consumer = new()
            {
                GroupId = "email-sending-consumer"
            },
            SendEmailQueueTopicName = EmailSendingConsumerTopic,
            EmailSendingAcceptedTopicName = EmailSendingAcceptedProducerTopic,
            Admin = new()
            {
                TopicList = new List<string> { EmailSendingConsumerTopic, EmailSendingAcceptedProducerTopic }
            }
        };

        IServiceCollection services = new ServiceCollection()
            .AddLogging()
            .AddSingleton(kafkaSettings)
            .AddSingleton<ICommonProducer, CommonProducer>()
            .AddSingleton(_emailServiceMock)
            .AddHostedService<EmailSendingConsumer>();

        _serviceProvider = services.BuildServiceProvider();
    }

    public async Task InitializeAsync()
    {
        await Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        await KafkaUtil.DeleteTopicAsync(EmailSendingConsumerTopic);
        await KafkaUtil.DeleteTopicAsync(EmailSendingAcceptedProducerTopic);
    }

    [Fact]
    public async Task ConsumeEmailTest_Successfull_deserialization_of_message_Service_called_once()
    {
        // Arrange
        Core.Models.Email email =
            new(Guid.NewGuid(), "test", "body", "fromAddress", "toAddress", EmailContentType.Plain);

        using CommonProducer kafkaProducer = KafkaUtil.GetKafkaProducer(_serviceProvider);
        using EmailSendingConsumer sut = GetEmailSendingConsumer();

        // Act
        await kafkaProducer.ProduceAsync(EmailSendingConsumerTopic, JsonSerializer.Serialize(email));

        await sut.StartAsync(CancellationToken.None);
        await Task.Delay(10000);
        await sut.StopAsync(CancellationToken.None);

        // Assert
        await _emailServiceMock.Received().SendAsync(Arg.Any<Core.Models.Email>());
    }

    [Fact]
    public async Task ConsumeEmailTest_Deserialization_of_message_fails_Never_calls_service()
    {
        // Arrange
        using CommonProducer kafkaProducer = KafkaUtil.GetKafkaProducer(_serviceProvider);
        using EmailSendingConsumer sut = GetEmailSendingConsumer();

        // Act
        await kafkaProducer.ProduceAsync(EmailSendingConsumerTopic, "Not an email");

        await sut.StartAsync(CancellationToken.None);
        await Task.Delay(10000);
        await sut.StopAsync(CancellationToken.None);

        // Assert
        await _emailServiceMock.DidNotReceive().SendAsync(Arg.Any<Core.Models.Email>());
    }

    private EmailSendingConsumer GetEmailSendingConsumer()
    {
        var emailSendingConsumer = _serviceProvider.GetService(typeof(IHostedService)) as EmailSendingConsumer;

        if (emailSendingConsumer == null)
        {
            Assert.Fail("Unable to create an instance of EmailSendingConsumer.");
        }

        return emailSendingConsumer;
    }
}
