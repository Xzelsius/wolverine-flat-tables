using Marten;
using Microsoft.AspNetCore.Mvc;
using Repro.Api.Domain;
using Wolverine.Http;

namespace Repro.Api.Endpoints;

public static class GetTodoEndpoint
{
    [WolverineGet("/todos/{todoId}")]
    public static Task<Todo?> Get([FromRoute] Guid todoId, IDocumentSession session)
    {
        return session.LoadAsync<Todo>(todoId);
    }
}
