use super::*;

pub fn supports_fast_path_projection(select: &Select) -> bool {
    select.projection.iter().all(|item| match item {
        SelectItem::Wildcard(_) => true,
        SelectItem::UnnamedExpr(Expr::Identifier(_))
        | SelectItem::UnnamedExpr(Expr::CompoundIdentifier(_)) => true,
        SelectItem::ExprWithAlias { expr, .. } => {
            matches!(expr, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
        }
        _ => false,
    })
}

pub fn supports_fast_path_filter(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                supports_fast_path_filter(left) && supports_fast_path_filter(right)
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq
            | BinaryOperator::Spaceship
            | BinaryOperator::Regexp
            | BinaryOperator::MyIntegerDivide
            | BinaryOperator::Xor
            | BinaryOperator::BitwiseAnd
            | BinaryOperator::BitwiseOr
            | BinaryOperator::BitwiseXor
            | BinaryOperator::AtArrow
            | BinaryOperator::Question => {
                supports_filter_value(left) && supports_filter_value(right)
            }
            _ => false,
        },
        Expr::RLike { expr, pattern, .. } => {
            supports_filter_value(expr) && supports_filter_value(pattern)
        }
        Expr::InList { expr, list, .. } => {
            expr_identifier_name(expr).is_ok() && list.iter().all(supports_filter_value)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_identifier_name(expr).is_ok()
                && supports_filter_value(low)
                && supports_filter_value(high)
        }
        Expr::IsNull(expr) | Expr::IsNotNull(expr) => supports_filter_value(expr),
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            supports_filter_value(expr) && supports_filter_value(pattern)
        }
        Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
            supports_filter_value(left) && supports_filter_value(right)
        }
        Expr::Nested(inner) => supports_fast_path_filter(inner),
        Expr::Function(_) => true,
        _ => false,
    }
}

pub(super) fn supports_filter_value(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(_)
        | Expr::CompoundIdentifier(_)
        | Expr::Value(_)
        | Expr::TypedString(_)
        | Expr::UnaryOp { .. }
        | Expr::Array(_)
        | Expr::Function(_)
        | Expr::Interval(_)
        | Expr::Substring { .. }
        | Expr::Trim { .. }
        | Expr::Ceil { .. }
        | Expr::Floor { .. }
        | Expr::Extract { .. }
        | Expr::Case { .. } => true,
        Expr::Collate { expr, .. } => supports_filter_value(expr),
        Expr::Cast { expr, .. } => supports_filter_value(expr),
        Expr::Nested(inner) => supports_filter_value(inner),
        _ => false,
    }
}

pub fn expr_identifier_name(expr: &Expr) -> PgWireResult<String> {
    match expr {
        Expr::Identifier(Ident { value, .. }) => Ok(value.clone()),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => Ok(parts[1].value.clone()),
        Expr::Cast { expr, .. } => expr_identifier_name(expr),
        Expr::Nested(inner) => expr_identifier_name(inner),
        _ => Err(unsupported("expected a simple column identifier")),
    }
}
