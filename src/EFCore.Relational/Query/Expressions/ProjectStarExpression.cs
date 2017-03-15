// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using JetBrains.Annotations;
using Microsoft.EntityFrameworkCore.Query.Sql;
using Microsoft.EntityFrameworkCore.Utilities;

namespace Microsoft.EntityFrameworkCore.Query.Expressions
{
    /// <summary>
    ///     A project star expression.
    /// </summary>
    public class ProjectStarExpression : Expression
    {
        private readonly List<Expression> _projections;
        private readonly TableExpressionBase _tableExpression;

        /// <summary>
        ///     Creates a new instance of a ProjectStarExpression.
        /// </summary>
        /// <param name="projections"> The list of existing expressions being used from this star projection. </param>
        /// <param name="tableExpression"> The target table expression being represented by this star projection. </param>
        public ProjectStarExpression(
            [NotNull] List<Expression> projections,
            [NotNull] TableExpressionBase tableExpression)
        {
            Check.NotNull(tableExpression, nameof(tableExpression));

            _projections = projections;
            _tableExpression = tableExpression;
        }
        /// <summary>
        ///     Gets or adds given expression in the list of projections being used from this star projection.
        /// </summary>
        /// <param name="expression"> The expression to be added to projections. </param>
        /// <returns>
        ///     The expression present in projections list.
        /// </returns>
        public virtual Expression GetOrAdd([NotNull] Expression expression)
        {
            var projection = _projections.Find(e => SelectExpression.ExpressionEqualityComparer.Equals(e, expression));

            if (projection != null)
            {
                return projection;
            }

            _projections.Add(expression);

            return expression;
        }

        /// <summary>
        ///     The target table being project starred.
        /// </summary>
        public virtual TableExpressionBase Table => _tableExpression;

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

        /// <summary>
        ///     Tests if this object is considered equal to another.
        /// </summary>
        /// <param name="obj"> The object to compare with the current object. </param>
        /// <returns>
        ///     true if the objects are considered equal, false if they are not.
        /// </returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return obj.GetType() == GetType() && Equals((ProjectStarExpression)obj);
        }

        private bool Equals([NotNull] ProjectStarExpression other)
            => _projections.SequenceEqual(other._projections)
               && _tableExpression.Equals(other._tableExpression);

        /// <summary>
        ///     Returns a hash code for this object.
        /// </summary>
        /// <returns>
        ///     A hash code for this object.
        /// </returns>
        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = _projections.Aggregate(0, (current, projection) => current + ((current * 397) ^ projection.GetHashCode()));

                return (hashCode * 397) ^ _tableExpression.GetHashCode();
            }
        }

        /// <summary>
        ///     Creates a <see cref="String" /> representation of the Expression.
        /// </summary>
        /// <returns>A <see cref="String" /> representation of the Expression.</returns>
        public override string ToString() => _tableExpression.Alias + ".*";
    }
}
