using Marten;
using Marten.Events.Projections;
using Oakton;
using Repro.Api.Domain;
using Repro.Api.Domain.Events;
using Weasel.Core;
using Wolverine;
using Wolverine.Http;
using Wolverine.Marten;

namespace Repro.Api;

public class Program
{
    public static Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        builder.Host.ApplyOaktonExtensions();
        builder.Host.UseWolverine(opts =>
        {
            opts.Policies.AutoApplyTransactions();
        });

        builder.Services.AddEndpointsApiExplorer();
        builder.Services.AddSwaggerGen();

        var marten = builder.Services.AddMarten(opts =>
        {
            opts.Connection(builder.Configuration.GetConnectionString("Marten")!);

            if (builder.Environment.IsDevelopment())
            {
                opts.AutoCreateSchemaObjects = AutoCreate.All;
            }

            opts.Projections.Snapshot<Todo>(SnapshotLifecycle.Inline, p => p.DeleteEvent<TodoDeleted>());
        });

        marten.UseLightweightSessions();

        marten.IntegrateWithWolverine().EventForwardingToWolverine();

        var app = builder.Build();

        app.UseSwagger();
        app.UseSwaggerUI();

        app.MapWolverineEndpoints();

        // redirect unspecific requests to swagger
        app.MapGet("/", () => Results.Redirect("/swagger"));

        return app.RunOaktonCommands(args);
    }
}
