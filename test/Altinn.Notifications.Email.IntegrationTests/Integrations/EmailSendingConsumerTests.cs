using System.Text.Json;

using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Integrations.Clients;
using Altinn.Notifications.Email.Integrations.Configuration;
using Altinn.Notifications.Email.Integrations.Consumers;
using Altinn.Notifications.Email.Integrations.Producers;
using Altinn.Notifications.Email.IntegrationTests.Utils;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using NSubstitute;

using Xunit;

namespace Altinn.Notifications.Email.IntegrationTests.Integrations;

public sealed class EmailSendingConsumerTests : IAsyncLifetime
{
    private readonly string TestTopic = Guid.NewGuid().ToString();

    private readonly IEmailService _emailServiceMock;

    private readonly ServiceProvider _serviceProvider;

    public EmailSendingConsumerTests()
    {
        Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Development");

        _emailServiceMock = Substitute.For<IEmailService>();

        var kafkaSettings = new KafkaSettings
        {
            BrokerAddress = "localhost:9092",
            EmailSendingConsumerSettings = new()
            {
                ConsumerGroupId = "email-sending-consumer",
                TopicName = TestTopic
            },
            EmailSendingAcceptedProducerSettings = new()
            {
                TopicName = TestTopic
            }
        };

        IServiceCollection services = new ServiceCollection()
            .AddLogging()
            .AddSingleton(kafkaSettings)
            .AddSingleton<IEmailSendingAcceptedProducer, EmailSendingAcceptedProducer>()
            .AddSingleton(_emailServiceMock)
            .AddHostedService<EmailSendingConsumer>();

        _serviceProvider = services.BuildServiceProvider();
    }

    public async Task InitializeAsync()
    {
        await KafkaUtil.CreateTopicsAsync(TestTopic);
    }

    public async Task DisposeAsync()
    {
        await KafkaUtil.DeleteTopicAsync(TestTopic);
    }

    [Fact]
    public async Task ConsumeEmailTest_Successfull_deserialization_of_message_Service_called_once()
    {
        // Arrange
        Core.Models.Email email =
            new(Guid.NewGuid(), "test", "body", "fromAddress", "toAddress", Core.Models.EmailContentType.Plain);

        using EmailSendingAcceptedProducer kafkaProducer = GetKafkaProducer();
        using EmailSendingConsumer sut = GetEmailSendingConsumer();

        // Act
        await kafkaProducer.ProduceAsync(JsonSerializer.Serialize(email));

        await sut.StartAsync(CancellationToken.None);
        await Task.Delay(10000);
        await sut.StopAsync(CancellationToken.None);

        // Assert
        await _emailServiceMock.Received().SendEmail(Arg.Any<Core.Models.Email>());
    }

    [Fact]
    public async Task ConsumeEmailTest_Deserialization_of_message_fails_Never_calls_service()
    {
        // Arrange
        using EmailSendingAcceptedProducer kafkaProducer = GetKafkaProducer();
        using EmailSendingConsumer sut = GetEmailSendingConsumer();

        // Act
        await kafkaProducer.ProduceAsync("Not an email");

        await sut.StartAsync(CancellationToken.None);
        await Task.Delay(10000);
        await sut.StopAsync(CancellationToken.None);

        // Assert
        await _emailServiceMock.DidNotReceive().SendEmail(Arg.Any<Core.Models.Email>());
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

    private EmailSendingAcceptedProducer GetKafkaProducer()
    {
        var kafkaProducer = _serviceProvider.GetService(typeof(IEmailSendingAcceptedProducer)) as EmailSendingAcceptedProducer;

        if (kafkaProducer == null)
        {
            Assert.Fail("Unable to create an instance of KafkaProducer.");
        }

        return kafkaProducer;
    }
}
