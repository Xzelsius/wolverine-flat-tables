using Marten;
using Microsoft.AspNetCore.Mvc;
using Repro.Api.Domain;
using Repro.Api.Domain.Events;
using System.ComponentModel.DataAnnotations;
using Wolverine.Http;
using Wolverine.Marten;

namespace Repro.Api.Endpoints;

public static class DeleteTodoEndpoint
{
    public static async Task<Todo?> Load(Guid todoId, IDocumentSession session)
    {
        return session.Load<Todo>(todoId);
    }

    [WolverineDelete("/todos/{todoId}"), EmptyResponse]
    public static DeleteDoc<Todo> Delete([FromRoute] Guid todoId, [NotBody][Required] Todo todo, IDocumentSession session)
    {
        session.Events.Append(todo.Id, new TodoDeleted(todo.Id));
        return MartenOps.Delete(todo);
    }
}
