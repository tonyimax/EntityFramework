// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Linq.Expressions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query.Expressions;
using Remotion.Linq.Clauses;

// ReSharper disable once CheckNamespace
namespace Microsoft.EntityFrameworkCore.Internal
{
    public static class RelationalExpressionExtensions
    {
        public static ColumnExpression TryGetColumnExpression([NotNull] this Expression expression)
            => expression as ColumnExpression
               ?? (expression as AliasExpression)?.TryGetColumnExpression()
               ?? (expression as NullableExpression)?.Operand.TryGetColumnExpression();

        public static IProperty TryGetProperty([NotNull] this Expression expression)
        {
            return (expression as ColumnExpression)?.Property
                   ?? (expression as AliasExpression)?.Expression.TryGetProperty()
                   ?? (expression as ColumnReferenceExpression)?.Expression.TryGetProperty();
        }

        public static IQuerySource TryGetQuerySource([NotNull] this Expression expression)
        {
            return (expression as ColumnExpression)?.Table.QuerySource
                   ?? (expression as ColumnReferenceExpression)?.Table.QuerySource
                   ?? (expression as AliasExpression)?.Expression.TryGetQuerySource();
        }

        public static ColumnReferenceExpression TryBindColumnReferenceExpression([NotNull] this Expression expression, TableExpressionBase table)
            => expression is AliasExpression ae
                ? new ColumnReferenceExpression(ae, table)
                : (expression is ColumnExpression ce
                    ? new ColumnReferenceExpression(ce, table)
                    : (expression is ColumnReferenceExpression cre
                        ? new ColumnReferenceExpression(cre, table)
                        : null));

        public static bool IsAliasWithColumnExpression([NotNull] this Expression expression)
            => (expression as AliasExpression)?.Expression is ColumnExpression;

        public static bool IsAliasWithSelectExpression([NotNull] this Expression expression)
            => (expression as AliasExpression)?.Expression is SelectExpression;

        public static bool HasColumnExpression([CanBeNull] this AliasExpression aliasExpression)
            => aliasExpression?.Expression is ColumnExpression;

        public static ColumnExpression TryGetColumnExpression([NotNull] this AliasExpression aliasExpression)
            => aliasExpression.Expression as ColumnExpression;

        public static bool IsSimpleExpression([NotNull] this Expression expression)
        {
            var unwrappedExpression = expression.RemoveConvert();

            return unwrappedExpression is ConstantExpression
                   || unwrappedExpression is ColumnExpression
                   || unwrappedExpression is ColumnReferenceExpression
                   || unwrappedExpression is ParameterExpression
                   || (unwrappedExpression as AliasExpression)?.Expression.IsSimpleExpression() == true;
        }
    }
}
