using System.Net;
using System.Text;

using Altinn.Notifications.Email.Core.Sending;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;

using Moq;
using Xunit;

namespace Altinn.Notifications.Email.IntegrationTests.DeliveryReportController;

public class DeliveryReportControllerTests : IClassFixture<IntegrationTestWebApplicationFactory<Controllers.DeliveryReportController>>
{
    private const string _basePath = "/notifications/email/api/v1/reports";

    private readonly string _eventMessageString = "[{\"id\": \"00000000-0000-0000-0000-000000000000\",\"topic\": \"/subscriptions/{subscription-id}/resourceGroups/{group-name}/providers/microsoft.communication/communicationservices/{communication-services-resource-name}\", \"subject\": \"sender/senderid@azure.com/message/00000000-0000-0000-0000-000000000000\", \"data\": {\"sender\": \"senderid@azure.com\", \"recipient\": \"receiver@azure.com\", \"messageId\": \"00000000-0000-0000-0000-000000000000\",\"status\": \"Delivered\", \"deliveryStatusDetails\": {\"statusMessage\": \"Status Message\"},\"deliveryAttemptTimeStamp\": \"2020-09-18T00:22:20.2855749+00:00\"},\"eventType\": \"Microsoft.Communication.EmailDeliveryReportReceived\",\"dataVersion\": \"1.0\",\"metadataVersion\": \"1\",\"eventTime\": \"2020-09-18T00:22:20.822Z\"}]";

    private readonly IntegrationTestWebApplicationFactory<Controllers.DeliveryReportController> _factory;

    public DeliveryReportControllerTests(IntegrationTestWebApplicationFactory<Controllers.DeliveryReportController> factory)
    {
        _factory = factory;
    }

    [Fact]
    public async Task Post_InvalidDeliveryReport_ReturnsBadRequest()
    {
        // Arrange
        HttpClient client = GetTestClient();
        HttpRequestMessage httpRequestMessage = new(HttpMethod.Post, _basePath)
        {
            Content = new StringContent("{something:wrong}", Encoding.UTF8, "application/json")
        };

        // Act
        HttpResponseMessage response = await client.SendAsync(httpRequestMessage);

        // Assert
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task Post_ValidDeliveryReport_ReturnsOK()
    {
        // Arrange
        HttpClient client = GetTestClient();
        HttpRequestMessage httpRequestMessage = new(HttpMethod.Post, _basePath)
        {
            Content = new StringContent(_eventMessageString, Encoding.UTF8, "application/json")
        };

        // Act
        HttpResponseMessage response = await client.SendAsync(httpRequestMessage);

        // Assert
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    private HttpClient GetTestClient()
    {
        HttpClient client = _factory.WithWebHostBuilder(builder =>
        {
            var sendingServiceMock = new Mock<ISendingService>();

            builder.ConfigureTestServices(services =>
            {
                services.AddSingleton(sendingServiceMock.Object);
            });
        }).CreateClient();

        return client;
    }
}
