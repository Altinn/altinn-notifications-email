using Altinn.Notifications.Email.Core;
using Altinn.Notifications.Email.Core.Configuration;
using Altinn.Notifications.Email.Core.Dependencies;
using Altinn.Notifications.Email.Core.Status;

using Azure.Messaging.EventGrid;

using Moq;
using Xunit;

namespace Altinn.Notifications.Email.Tests.Email.Core.Sending;

public class StatusServiceTests
{
    private readonly TopicSettings _topicSettings;
    private readonly string _validationEvent = "[{\"id\": \"2d1781af-3a4c-4d7c-bd0c-e34b19da4e66\",\"topic\": \"/subscriptions/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\",\"subject\": \"\",\"data\": {\"validationCode\": \"512d38b6-c7b8-40c8-89fe-f46f9e9622b6\",\"validationUrl\": \"https://rp-eastus2.eventgrid.azure.net:553/eventsubscriptions/myeventsub/validate?id=0000000000-0000-0000-0000-00000000000000&t=2022-10-28T04:23:35.1981776Z&apiVersion=2018-05-01-preview&token=1A1A1A1A\"},\"eventType\": \"Microsoft.EventGrid.SubscriptionValidationEvent\",\"eventTime\": \"2022-10-28T04:23:35.1981776Z\",\"metadataVersion\": \"1\",\"dataVersion\": \"1\"}]";
    private readonly string _deliveryReportEvent = "[{\"id\": \"00000000-0000-0000-0000-000000000000\",\"topic\": \"/subscriptions/{subscription-id}/resourceGroups/{group-name}/providers/microsoft.communication/communicationservices/{communication-services-resource-name}\", \"subject\": \"sender/senderid@azure.com/message/00000000-0000-0000-0000-000000000000\", \"data\": {\"sender\": \"senderid@azure.com\", \"recipient\": \"receiver@azure.com\", \"messageId\": \"00000000-0000-0000-0000-000000000000\",\"status\": \"Delivered\", \"deliveryStatusDetails\": {\"statusMessage\": \"Status Message\"},\"deliveryAttemptTimeStamp\": \"2020-09-18T00:22:20.2855749+00:00\"},\"eventType\": \"Microsoft.Communication.EmailDeliveryReportReceived\",\"dataVersion\": \"1.0\",\"metadataVersion\": \"1\",\"eventTime\": \"2020-09-18T00:22:20.822Z\"}]";

    public StatusServiceTests()
    {
        _topicSettings = new()
        {
            EmailStatusUpdatedTopicName = "EmailStatusUpdatedTopicName",
            EmailSendingAcceptedTopicName = "EmailSendingAcceptedTopicName"
        };
    }

    [Fact]
    public async Task UpdateSendStatus_OperationResultGenerated_PublishedToExpectedKafkaTopic()
    {
        // Arrange
        Guid id = Guid.NewGuid();
        SendNotificationOperationIdentifier identifier = new()
        {
            OperationId = "operation-id",
            NotificationId = id
        };

        Mock<IEmailServiceClient> clientMock = new();
        clientMock.Setup(c => c.GetOperationUpdate(It.IsAny<string>()))
            .ReturnsAsync(EmailSendResult.Delivered);

        Mock<ICommonProducer> producerMock = new();
        producerMock.Setup(p => p.ProduceAsync(
            It.Is<string>(s => s.Equals(nameof(_topicSettings.EmailStatusUpdatedTopicName))),
            It.Is<string>(s =>
                s.Contains("\"operationId\":\"operation-id\"") &&
                s.Contains("\"sendResult\":\"Delivered\"") &&
                s.Contains($"\"notificationId\":\"{id}\""))));

        Mock<IDateTimeService> dateTimeMock = new();

        var sut = new StatusService(clientMock.Object, producerMock.Object, dateTimeMock.Object, _topicSettings);

        // Act
        await sut.UpdateSendStatus(identifier);

        // Assert
        producerMock.VerifyAll();
    }

    [Fact]
    public async Task UpdateSendStatus_SendResultIsSending_LatStatusCheckUpdatedAndPublishedBackOnTopic()
    {
        // Arrange
        Guid id = Guid.NewGuid();
        SendNotificationOperationIdentifier identifier = new()
        {
            OperationId = "operation-id",
            NotificationId = id,
            LastStatusCheck = new DateTime(1994, 06, 16, 08, 00, 00, DateTimeKind.Utc)
        };

        string actualProducerInput = string.Empty;

        Mock<IEmailServiceClient> clientMock = new();
        clientMock.Setup(c => c.GetOperationUpdate(It.IsAny<string>()))
            .ReturnsAsync(EmailSendResult.Sending);

        Mock<ICommonProducer> producerMock = new();
        producerMock.Setup(p => p.ProduceAsync(
            It.Is<string>(s => s.Equals(nameof(_topicSettings.EmailSendingAcceptedTopicName))),
            It.IsAny<string>()))
              .Callback<string, string>((topicName, serializedIdentifier) =>
              {
                  actualProducerInput = serializedIdentifier;
              });

        Mock<IDateTimeService> dateTimeMock = new();
        dateTimeMock.Setup(d => d.UtcNow()).Returns(new DateTime(2023, 06, 16, 08, 00, 00, DateTimeKind.Utc));

        var sut = new StatusService(clientMock.Object, producerMock.Object, dateTimeMock.Object, _topicSettings);

        // Act
        await sut.UpdateSendStatus(identifier);

        // Assert
        producerMock.VerifyAll();
        Assert.Contains("\"operationId\":\"operation-id\"", actualProducerInput);
        Assert.Contains($"\"notificationId\":\"{id}\"", actualProducerInput);
        Assert.Contains("\"lastStatusCheck\":\"2023-06-16T08:00:00Z\"", actualProducerInput);
    }

    [Fact]
    public async Task ProcessDeliveryReports_EventTypeIsValidation_ValidationResponseReturned()
    {
        Mock<IEmailServiceClient> clientMock = new();
        Mock<ICommonProducer> producerMock = new();
        Mock<IDateTimeService> dateTimeMock = new();

        StatusService statusService = new(clientMock.Object, producerMock.Object, dateTimeMock.Object, _topicSettings);

        string result = await statusService.ProcessDeliveryReports(EventGridEvent.ParseMany(BinaryData.FromString(_validationEvent)));

        Assert.NotNull(result);
        Assert.Contains("\"validationResponse\":\"512d38b6-c7b8-40c8-89fe-f46f9e9622b6\"", result);
    }

    [Fact]
    public async Task ProcessDeliveryReports_OperationResultGenerated_PublishedToExpectedKafkaTopic()
    {
        Mock<IEmailServiceClient> clientMock = new();

        Mock<ICommonProducer> producerMock = new();
        producerMock.Setup(p => p.ProduceAsync(
            It.Is<string>(s => s.Equals(nameof(_topicSettings.EmailStatusUpdatedTopicName))),
            It.Is<string>(s =>
                s.Contains("\"operationId\":\"00000000-0000-0000-0000-000000000000\"") &&
                s.Contains("\"sendResult\":\"Delivered\""))));
        
        Mock<IDateTimeService> dateTimeMock = new();

        StatusService statusService = new(clientMock.Object, producerMock.Object, dateTimeMock.Object, _topicSettings);

        string result = await statusService.ProcessDeliveryReports(EventGridEvent.ParseMany(BinaryData.FromString(_deliveryReportEvent)));

        producerMock.VerifyAll();
        Assert.NotNull(result);
    }
}
