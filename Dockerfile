# Use the official .NET SDK image with Alpine Linux as a base image
FROM mcr.microsoft.com/dotnet/sdk:7.0.203-alpine3.17 AS build

# Set the working directory in the container
WORKDIR /app

# Copy csproj and restore as distinct layers
COPY /src/Altinn.Notifications.Email/*.csproj ./src/Altinn.Notifications.Email/
COPY /src/Altinn.Notifications.Email.Core/*.csproj ./src/Altinn.Notifications.Email.Core/
COPY /src/Altinn.Notifications.Email.Integrations/*.csproj ./src/Altinn.Notifications.Email.Integrations/

RUN dotnet restore ./src/Altinn.Notifications.Email/Altinn.Notifications.Email.csproj

# Copy the remaining source code and build the application
COPY /src ./src
RUN dotnet publish -c Release -o out ./src/Altinn.Notifications.Email/Altinn.Notifications.Email.csproj


# Use the official .NET runtime image with Alpine Linux as a base image
FROM mcr.microsoft.com/dotnet/aspnet:7.0.5-alpine3.17 AS final

# Set the working directory in the container
WORKDIR /app

# Copy the built application from the build environment
COPY --from=build /app/out ./

# Run the application
ENTRYPOINT [ "dotnet", "Altinn.Notifications.Email.dll" ]