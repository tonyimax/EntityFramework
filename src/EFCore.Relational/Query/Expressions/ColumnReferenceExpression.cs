// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Linq.Expressions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Query.Sql;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Query.Expressions
{
    /// <summary>
    ///     A column expression.
    /// </summary>
    public class ColumnReferenceExpression : Expression
    {
       private readonly Expression _expression;
        private readonly TableExpressionBase _tableExpression;
        private readonly Type _type;
        
        /// <summary>
        ///     Creates a new instance of a ColumnExpression.
        /// </summary>
        /// <param name="aliasExpression"> The corresponding property. </param>
        /// <param name="tableExpression"> The target table expression. </param>
        public ColumnReferenceExpression(
            [NotNull] AliasExpression aliasExpression,
            [NotNull] TableExpressionBase tableExpression)
            : this(
                  aliasExpression.Alias,
                  aliasExpression.Type,
                  Check.NotNull(aliasExpression, nameof(aliasExpression)),
                  Check.NotNull(tableExpression, nameof(tableExpression)))
        {
        }

        /// <summary>
        ///     Creates a new instance of a ColumnExpression.
        /// </summary>
        /// <param name="columnExpression"> The corresponding property. </param>
        /// <param name="tableExpression"> The target table expression. </param>
        public ColumnReferenceExpression(
            [NotNull] ColumnExpression columnExpression,
            [NotNull] TableExpressionBase tableExpression)
            : this(
                  columnExpression.Name,
                  columnExpression.Type,
                  Check.NotNull(columnExpression, nameof(columnExpression)),
                  Check.NotNull(tableExpression, nameof(tableExpression)))
        {
        }

        /// <summary>
        ///     Creates a new instance of a ColumnExpression.
        /// </summary>
        /// <param name="columnReferenceExpression"> The corresponding property. </param>
        /// <param name="tableExpression"> The target table expression. </param>
        public ColumnReferenceExpression(
            [NotNull] ColumnReferenceExpression columnReferenceExpression,
            [NotNull] TableExpressionBase tableExpression)
            : this(
                  columnReferenceExpression.Name,
                  columnReferenceExpression.Type,
                  Check.NotNull(columnReferenceExpression, nameof(columnReferenceExpression)),
                  Check.NotNull(tableExpression, nameof(tableExpression)))
        {
        }

        /// <summary>
        ///     Creates a new instance of a ColumnExpression.
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="type"></param>
        /// <param name="projectStarExpression"> The corresponding property. </param>
        /// <param name="tableExpression"> The target table expression. </param>
        public ColumnReferenceExpression(
            [NotNull] string columnName,
            [NotNull] Type type,
            [NotNull] ProjectStarExpression projectStarExpression,
            [NotNull] TableExpressionBase tableExpression)
            : this(
                  Check.NotEmpty(columnName, nameof(columnName)),
                  Check.NotNull(type, nameof(type)),
                  Check.NotNull(projectStarExpression, nameof(projectStarExpression)) as Expression, 
                  Check.NotNull(tableExpression, nameof(tableExpression)))
        {
        }


        private ColumnReferenceExpression(string name, Type type, Expression expression, TableExpressionBase tableExpression)
        {
            Name = name;
            _type = type;
            _expression = expression;
            _tableExpression = tableExpression;
        }

        /// <summary>
        ///     The target table.
        /// </summary>
        public virtual TableExpressionBase Table => _tableExpression;

        /// <summary>
        ///     The target table alias.
        /// </summary>
        public virtual string TableAlias => _tableExpression.Alias;

        /// <summary>
        ///     The target table alias.
        /// </summary>
        public virtual Expression Expression => _expression;

        /// <summary>
        ///     Gets the column name.
        /// </summary>
        /// <value>
        ///     The column name.
        /// </value>
        public virtual string Name { get; }

        /// <summary>
        ///     Gets the static type of the expression that this <see cref="Expression" /> represents. (Inherited from <see cref="Expression" />.)
        /// </summary>
        /// <returns> The <see cref="Type" /> that represents the static type of the expression. </returns>
        public override Type Type => _type;

        /// <summary>
        ///     Returns the node type of this <see cref="Expression" />. (Inherited from <see cref="Expression" />.)
        /// </summary>
        /// <returns> The <see cref="ExpressionType" /> that represents this expression. </returns>
        public override ExpressionType NodeType => ExpressionType.Extension;

        /// <summary>
        ///     Dispatches to the specific visit method for this node type.
        /// </summary>
        protected override Expression Accept(ExpressionVisitor visitor)
        {
            Check.NotNull(visitor, nameof(visitor));

            var specificVisitor = visitor as ISqlExpressionVisitor;

            return specificVisitor != null
                ? specificVisitor.VisitColumnReference(this)
                : base.Accept(visitor);
        }

        /// <summary>
        ///     Reduces the node and then calls the <see cref="ExpressionVisitor.Visit(Expression)" /> method passing the
        ///     reduced expression.
        ///     Throws an exception if the node isn't reducible.
        /// </summary>
        /// <param name="visitor"> An instance of <see cref="ExpressionVisitor" />. </param>
        /// <returns> The expression being visited, or an expression which should replace it in the tree. </returns>
        /// <remarks>
        ///     Override this method to provide logic to walk the node's children.
        ///     A typical implementation will call visitor.Visit on each of its
        ///     children, and if any of them change, should return a new copy of
        ///     itself with the modified children.
        /// </remarks>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

        protected bool Equals(ColumnReferenceExpression other)
            => Equals(_expression, other._expression) 
                && Equals(_tableExpression, other._tableExpression) 
            && _type == other._type 
            && string.Equals(Name, other.Name);

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            return obj.GetType() == GetType() && Equals((ColumnReferenceExpression)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = _expression?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (_tableExpression?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (_type?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Name?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        /// <summary>
        ///     Creates a <see cref="String" /> representation of the Expression.
        /// </summary>
        /// <returns>A <see cref="String" /> representation of the Expression.</returns>
        public override string ToString() => _tableExpression.Alias + "." + Name;
    }
}
