use daft_dsl::expr::{Expr, ExprRef, Operator};
use sqlparser::ast::{
    BinaryOperator, Expr as SQLExpr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    ObjectName, ObjectNamePart, UnaryOperator, Value, ValueWithSpan,
};

#[derive(Debug, Clone)]
enum NameExpr {
    Ident(String),
    CompoundIdent(Vec<String>),
    Literal(String),
    Unary { op: String, expr: Box<NameExpr> },
    Binary {
        left: Box<NameExpr>,
        op: String,
        right: Box<NameExpr>,
    },
    FuncCall { name: String, args: Vec<NameExpr> },
    Wildcard,
    QualifiedWildcard(String),
    Cast {
        expr: Box<NameExpr>,
        data_type: String,
    },
}

fn format_name_expr(expr: &NameExpr) -> String {
    match expr {
        NameExpr::Ident(name) => name.clone(),
        NameExpr::CompoundIdent(parts) => parts.join("."),
        NameExpr::Literal(s) => s.clone(),

        NameExpr::Unary { op, expr } => format!("({op} {})", format_name_expr(expr)),
        NameExpr::Binary { left, op, right } => format!(
            "({} {op} {})",
            format_name_expr(left),
            format_name_expr(right)
        ),

        NameExpr::FuncCall { name, args } => {
            if name == "count" && args.len() == 1 && matches!(&args[0], NameExpr::Wildcard) {
                return "count(*)".to_string();
            }

            let args = args.iter().map(format_name_expr).collect::<Vec<_>>();
            format!("{name}({})", args.join(", "))
        }

        NameExpr::Wildcard => "*".to_string(),
        NameExpr::QualifiedWildcard(prefix) => format!("{prefix}.*"),

        NameExpr::Cast { expr, data_type } => {
            format!("cast({} as {data_type})", format_name_expr(expr))
        }
    }
}

fn ident_to_string(ident: &Ident) -> String {
    ident.value.clone()
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => ident_to_string(ident),
            ObjectNamePart::Function(func) => func.name.value.clone(),
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::Number(n, _) => n.clone(),
        Value::Boolean(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => format!("\"{s}\""),
        other => format!("{other}"),
    }
}

fn value_with_span_to_string(v: &ValueWithSpan) -> String {
    value_to_string(&v.value)
}

fn sql_binary_op_to_string(op: &BinaryOperator) -> String {
    match op {
        BinaryOperator::Plus => "+".to_string(),
        BinaryOperator::Minus => "-".to_string(),
        BinaryOperator::Multiply => "*".to_string(),
        BinaryOperator::Divide => "/".to_string(),
        BinaryOperator::Modulo => "%".to_string(),
        BinaryOperator::StringConcat => "||".to_string(),
        other => format!("{other}"),
    }
}

fn sql_unary_op_to_string(op: &UnaryOperator) -> String {
    match op {
        UnaryOperator::Plus => "+".to_string(),
        UnaryOperator::Minus => "-".to_string(),
        UnaryOperator::Not => "not".to_string(),
        other => format!("{other}"),
    }
}

fn sql_expr_to_name_expr(e: &SQLExpr) -> Option<NameExpr> {
    match e {
        SQLExpr::Identifier(ident) => Some(NameExpr::Ident(ident_to_string(ident))),
        SQLExpr::CompoundIdentifier(idents) => Some(NameExpr::CompoundIdent(
            idents.iter().map(ident_to_string).collect::<Vec<_>>(),
        )),
        SQLExpr::Value(v) => Some(NameExpr::Literal(value_with_span_to_string(v))),

        SQLExpr::UnaryOp { op, expr } => Some(NameExpr::Unary {
            op: sql_unary_op_to_string(op),
            expr: Box::new(sql_expr_to_name_expr(expr)?),
        }),
        SQLExpr::BinaryOp { left, op, right } => Some(NameExpr::Binary {
            left: Box::new(sql_expr_to_name_expr(left)?),
            op: sql_binary_op_to_string(op),
            right: Box::new(sql_expr_to_name_expr(right)?),
        }),

        SQLExpr::Nested(inner) => sql_expr_to_name_expr(inner),
        SQLExpr::Cast {
            expr, data_type, ..
        } => Some(NameExpr::Cast {
            expr: Box::new(sql_expr_to_name_expr(expr)?),
            data_type: data_type.to_string(),
        }),

        SQLExpr::Function(func) => {
            let name = object_name_to_string(&func.name).to_lowercase();
            let args: Vec<NameExpr> = match &func.args {
                FunctionArguments::None => vec![],
                FunctionArguments::Subquery(_) => vec![NameExpr::Literal("<subquery>".to_string())],
                FunctionArguments::List(args) => {
                    let mut out = Vec::with_capacity(args.args.len());
                    for arg in &args.args {
                        let arg = match arg {
                            FunctionArg::Named { arg, .. }
                            | FunctionArg::ExprNamed { arg, .. }
                            | FunctionArg::Unnamed(arg) => arg,
                        };

                        let name_arg = match arg {
                            FunctionArgExpr::Expr(e) => sql_expr_to_name_expr(e)?,
                            FunctionArgExpr::QualifiedWildcard(obj) => {
                                NameExpr::QualifiedWildcard(object_name_to_string(obj))
                            }
                            FunctionArgExpr::Wildcard => NameExpr::Wildcard,
                        };
                        out.push(name_arg);
                    }
                    out
                }
            };

            Some(NameExpr::FuncCall { name, args })
        }

        _ => None,
    }
}

pub fn normalized_sql_expr_name(expr: &SQLExpr) -> String {
    if let Some(name_expr) = sql_expr_to_name_expr(expr) {
        return format_name_expr(&name_expr);
    }

    // Fallback to sqlparser Display to avoid reducing supported queries.
    format!("{expr}")
}

fn dsl_op_to_string(op: &Operator) -> Option<String> {
    let s = match op {
        Operator::Plus => "+",
        Operator::Minus => "-",
        Operator::Multiply => "*",
        Operator::TrueDivide => "/",
        _ => return None,
    };

    Some(s.to_string())
}

fn dsl_expr_to_name_expr(expr: &ExprRef) -> Option<NameExpr> {
    match expr.as_ref() {
        Expr::Column(col) => Some(NameExpr::Ident(col.name())),
        Expr::Alias(_, name) => Some(NameExpr::Ident(name.to_string())),
        Expr::Literal(lit) => {
            if let daft_core::lit::Literal::Utf8(s) = lit {
                Some(NameExpr::Literal(format!("\"{s}\"")))
            } else {
                Some(NameExpr::Literal(lit.to_string()))
            }
        }

        Expr::Not(inner) => Some(NameExpr::Unary {
            op: "not".to_string(),
            expr: Box::new(dsl_expr_to_name_expr(inner)?),
        }),

        Expr::BinaryOp { op, left, right } => Some(NameExpr::Binary {
            left: Box::new(dsl_expr_to_name_expr(left)?),
            op: dsl_op_to_string(op)?,
            right: Box::new(dsl_expr_to_name_expr(right)?),
        }),

        Expr::Cast(child, dtype) => Some(NameExpr::Cast {
            expr: Box::new(dsl_expr_to_name_expr(child)?),
            data_type: dtype.to_string(),
        }),

        // For now we keep the DSL coverage intentionally small and rely on fallback
        // to cover all remaining expressions.
        _ => None,
    }
}

/// Public: normalized name for an unnamed DataFrame projection expression.
///
/// Always returns a name. If the expression isn't yet handled by the shared formatter,
/// falls back to the DSL Display string.
pub fn normalized_daft_expr_name(expr: &ExprRef) -> String {
    if let Some(name_expr) = dsl_expr_to_name_expr(expr) {
        return format_name_expr(&name_expr);
    }

    expr.to_string()
}

