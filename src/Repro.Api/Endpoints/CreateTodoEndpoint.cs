using Repro.Api.Domain;
using Repro.Api.Domain.Events;
using Repro.Api.Endpoints.Models;
using Wolverine.Http;
using Wolverine.Marten;

namespace Repro.Api.Endpoints;

public static class CreateTodoEndpoint
{
    [WolverinePut("/todos")]
    public static (CreateTodoResponse, IStartStream) Create(CreateTodoRequest request)
    {
        var created = new TodoCreated(Guid.NewGuid(), request.Description);
        return (new CreateTodoResponse(created.TodoId, created.Description), MartenOps.StartStream<Todo>(created.TodoId, created));
    }
}
