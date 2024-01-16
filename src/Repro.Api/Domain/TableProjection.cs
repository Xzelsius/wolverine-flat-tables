namespace Repro.Api.Domain;

using Marten;
using Marten.Events.Projections;
using SqlKata;
using SqlKata.Compilers;
using Weasel.Core;
using Weasel.Postgresql.Tables;

public record RowCreated(Guid Id, string Description, DateTimeOffset CreatedAt);

public record RowUpdated(
    Guid Id,
    string Description,
    string? Remarks,
    Guid SomeGuid,
    Guid? SomeNullableGuid,
    int SomeInt,
    int? SomeNullableInt,
    double SomeDouble,
    double? SomeNullableDouble,
    DateTimeOffset ModifiedAt);

public class TableProjection : EventProjection
{
    private readonly DbObjectName _tableIdentifier;

    public TableProjection()
    {
        var table = new Table("mt_table");

        table.AddColumn<Guid>("id").AsPrimaryKey();
        table.AddColumn<string>("description");
        table.AddColumn<string?>("remarks").AllowNulls();
        table.AddColumn<Guid>("some_guid");
        table.AddColumn<Guid?>("some_nullable_guid").AllowNulls();
        table.AddColumn<int>("some_int");
        table.AddColumn<int?>("some_nullable_int").AllowNulls();
        table.AddColumn<double>("some_double");
        table.AddColumn<double?>("some_nullable_double").AllowNulls();
        table.AddColumn<DateTimeOffset>("created_at");
        table.AddColumn<DateTimeOffset?>("modified_at").AllowNulls();

        _tableIdentifier = table.Identifier;
        SchemaObjects.Add(table);
    }

    public void Project(RowCreated @event, IDocumentOperations ops)
    {
        var sql = ToStatement(
            new Query(_tableIdentifier.ToString())
                .AsInsert(new
                {
                    id = "?",
                    description = "?",
                    created_at = "?"
                }));

        ops.QueueSqlCommand(sql, @event.Id, @event.Description, @event.CreatedAt);
    }

    public void Project(RowUpdated @event, IDocumentOperations ops)
    {
        var sql = ToStatement(
            new Query(_tableIdentifier.ToString())
                .Where(new { id = "?" })
                .AsUpdate(new
                {
                    description = "?",
                    remarks = "?",
                    some_guid = "?",
                    some_nullable_guid = "?",
                    some_int = "?",
                    some_nullable_int = "?",
                    some_double = "?",
                    some_nullable_double = "?",
                    modified_at = "?",
                }));

        ops.QueueSqlCommand(
            sql,
            @event.Description,
            @event.Remarks ?? (object)DBNull.Value,
            @event.SomeGuid,
            @event.SomeNullableGuid ?? (object)DBNull.Value,
            @event.SomeInt,
            @event.SomeNullableInt ?? (object)DBNull.Value,
            @event.SomeDouble,
            @event.SomeNullableDouble ?? (object)DBNull.Value,
            @event.ModifiedAt,
            @event.Id);
    }

    private static string ToStatement(Query query)
    {
        var compiled = new PostgresCompiler().Compile(query);
        return compiled.RawSql;
    }
}
