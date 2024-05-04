use std::ops::Range;

use anyhow::{anyhow, Context};
use logos::Logos;

#[derive(Debug, Clone, Copy, Logos)]
#[logos(skip r"[ \t\r\n\f]+")]
pub enum Token {
    /// Basic
    #[token("put")]
    Put,
    #[token("delete")]
    Delete,
    #[token("get")]
    Get,
    #[token("fill")]
    Fill,
    #[token("scan")]
    Scan,
    #[token("keys")]
    Keys,
    /// profiler
    #[token("show")]
    Show,
    #[token("profile")]
    Profile,
    /// Aplication
    #[token("channel")]
    Channel,
}

pub const VALID_TOKENS: &[&str] = &[
    "put",
    "delete",
    "get",
    "fill",
    "scan",
    "keys",
    "show",
    "profile",
    "channel",
    "unbounded",
    "in:",
    "ex:",
    "memtable",
    "sstable",
    "level",
    "options",
    "files",
    "add",
    "pub",
    "sub",
    "prevmsg-sub",
];

pub struct TokenError {
    pub msg: String,
    pub span: Range<usize>,
}

pub type Result<T, E = TokenError> = std::result::Result<T, E>;

pub fn parse_token(text: &str) -> Result<(Token, &str)> {
    let mut lexer = Token::lexer(text);
    if let Some(v) = lexer.next() {
        match v {
            Ok(v) => Ok((v, &text[lexer.span().end..])),
            _ => Err(TokenError {
                msg: format!("unexpected token here (may be you want input: {VALID_TOKENS:?})",),
                span: lexer.span(),
            }),
        }
    } else {
        Err(TokenError {
            msg: "empty values are not allowed".to_owned(),
            span: lexer.span(),
        })
    }
}

pub fn parse_token_with_report(text: &str) -> anyhow::Result<(Token, &str)> {
    match parse_token(text) {
        Ok((token, next)) => Ok((token, next)),
        Err(TokenError { msg, span }) => {
            use ariadne::{ColorGenerator, Label, Report, ReportKind, Source};

            let mut colors = ColorGenerator::new();

            let a = colors.next();

            Report::build(ReportKind::Error, (), span.start)
                .with_message("Invalid Syntax".to_string())
                .with_label(Label::new(span).with_message(&msg).with_color(a))
                .finish()
                .eprint(Source::from(text))
                .context("with")?;
            Err(anyhow!("Invalid Syntax: {msg}"))
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_token() {
        let p = "abc";
        let bound = 0..1;
        println!("{}", &p[bound.clone()]);
        println!("{}", &p[bound.end..]);
    }
}
