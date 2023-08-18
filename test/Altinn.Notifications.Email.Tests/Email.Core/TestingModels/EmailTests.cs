using System.Text.Json.Nodes;

using Altinn.Notifications.Email.Core.Models;

using Xunit;

namespace Altinn.Notifications.Tests.Notifications.Core.TestingModels;
public class EmailTests
{
    private readonly string _serializedEmail;

    public EmailTests()
    {
        Guid id = Guid.NewGuid();
        _serializedEmail = new JsonObject()
        {
            { "id", id },
            { "subject", "subject" },
            { "body", "body" },
            { "fromAddress", "from@domain.com" },
            { "toAddress", "to@domain.com" },
            { "contentType", "Html" }
        }.ToJsonString();
    }

    [Fact]
    public void TryParse_ValidEmail_True()
    {
        bool actualResult = Email.Core.Models.Email.TryParse(_serializedEmail, out Email.Core.Models.Email actual);
        Assert.True(actualResult);
        Assert.Equal("subject", actual.Subject);
        Assert.Equal(EmailContentType.Html, actual.ContentType);
    }

    [Fact]
    public void TryParse_EmptyString_False()
    {
        bool actualResult = Email.Core.Models.Email.TryParse(string.Empty, out _);
        Assert.False(actualResult);
    }

    [Fact]
    public void TryParse_InvalidString_False()
    {
        bool actualResult = Email.Core.Models.Email.TryParse("{\"ticket\":\"noTicket\"}", out _);

        Assert.False(actualResult);
    }

    [Fact]
    public void TryParse_InvalidJsonExceptionThrown_False()
    {
        bool actualResult = Email.Core.Models.Email.TryParse("{\"ticket:\"noTicket\"}", out _);

        Assert.False(actualResult);
    }
}
