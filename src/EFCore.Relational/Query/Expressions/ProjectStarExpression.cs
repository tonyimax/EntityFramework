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
    public class ProjectStarExpression : Expression
    {
        private readonly TableExpressionBase _tableExpression;

        /// <summary>
        ///     Creates a new instance of a ColumnExpression.
        /// </summary>
        /// <param name="tableExpression"> The target table expression. </param>
        public ProjectStarExpression([NotNull] TableExpressionBase tableExpression)
        {
            Check.NotNull(tableExpression, nameof(tableExpression));

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
        ///     Gets the static type of the expression that this <see cref="Expression" /> represents. (Inherited from <see cref="Expression" />.)
        /// </summary>
        /// <returns> The <see cref="Type" /> that represents the static type of the expression. </returns>
        public override Type Type => typeof(object);

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
                ? specificVisitor.VisitProjectStar(this)
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

        protected bool Equals(ProjectStarExpression other) => _tableExpression.Alias.Equals(other._tableExpression.Alias);

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            return obj.GetType() == this.GetType() && Equals((ProjectStarExpression)obj);
        }

        public override int GetHashCode() => _tableExpression.GetHashCode();

        /// <summary>
        ///     Creates a <see cref="String" /> representation of the Expression.
        /// </summary>
        /// <returns>A <see cref="String" /> representation of the Expression.</returns>
        public override string ToString() => _tableExpression.Alias + ".*";
    }
}
