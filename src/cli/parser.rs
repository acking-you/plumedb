use anyhow::anyhow;
use nom::branch::alt;
use nom::bytes::complete::{is_not, tag};
use nom::character::complete::space1;
use nom::combinator::{cut, opt};
use nom::error::{context, convert_error, ContextError, ParseError, VerboseError};
use nom::multi::many0;
use nom::sequence::{delimited, pair, preceded, separated_pair};
use nom::Err;
use volo_gen::plumedb::{Bound as GrpcBound, Unbounded};

use super::token::Token;
use crate::cli::token::parse_token_with_report;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Bound {
    UnBounded,
    Include(String),
    Exclude(String),
}

impl Bound {
    pub fn into_grpc_bound(self) -> GrpcBound {
        match self {
            Bound::UnBounded => GrpcBound {
                value: Some(volo_gen::plumedb::bound::Value::Unbouned(Unbounded {})),
            },
            Bound::Include(value) => GrpcBound {
                value: Some(volo_gen::plumedb::bound::Value::Include(value.into())),
            },
            Bound::Exclude(value) => GrpcBound {
                value: Some(volo_gen::plumedb::bound::Value::Exclude(value.into())),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BasicOp {
    Put(String, String),
    Delete(String),
    Get(String),
    Fill(Vec<(String, String)>),
    Scan(Bound, Bound),
    Keys(Bound, Bound),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShowItem {
    MemTable,
    SSTable,
    Level,
    Options,
    Files,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelOp {
    Show,
    Add(String),
    Publish(String, Vec<String>),
    Subcribe(bool, String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommonOp {
    Basic(BasicOp),
    Profile(BasicOp),
    Show(ShowItem),
    Channel(ChannelOp),
}

pub type IResult<'a, T, E> = nom::IResult<&'a str, T, E>;

fn sp<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, E> {
    space1(i)
}

fn not_sp<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, E> {
    is_not(" \t")(i)
}

fn not_sp_and_brace_and_comma<'a, E: ParseError<&'a str>>(i: &'a str) -> IResult<&'a str, E> {
    is_not(" \t(),")(i)
}

fn put<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<'a, (String, String), E> {
    use nom::combinator::map;
    let (next, (key, value)) = pair(
        context("key", preceded(sp, map(not_sp, |v| v.to_string()))),
        context("value", preceded(sp, map(not_sp, |v| v.to_string()))),
    )(i)?;
    Ok((next, (key, value)))
}

fn delete<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<'a, String, E> {
    use nom::combinator::map;
    let (next, key) = context("deleted key", preceded(sp, map(not_sp, |v| v.to_string())))(i)?;
    Ok((next, key))
}

fn get<'a, E: ParseError<&'a str> + ContextError<&'a str>>(i: &'a str) -> IResult<'a, String, E> {
    use nom::combinator::map;
    let (next, key) = context("get key", preceded(sp, map(not_sp, |v| v.to_string())))(i)?;
    Ok((next, key))
}

fn key_value<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<'a, (String, String), E> {
    use nom::character::complete::char;
    use nom::combinator::map;

    preceded(
        sp,
        delimited(
            char('('),
            map(
                separated_pair(
                    preceded(opt(sp), not_sp_and_brace_and_comma),
                    cut(preceded(opt(sp), char(','))),
                    preceded(opt(sp), not_sp_and_brace_and_comma),
                ),
                |(key, value)| (key.to_string(), value.to_string()),
            ),
            char(')'),
        ),
    )(i)
}

fn fill<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<'a, Vec<(String, String)>, E> {
    many0(key_value)(i)
}

fn bound<'a, E: ParseError<&'a str> + ContextError<&'a str>>(i: &'a str) -> IResult<'a, Bound, E> {
    use nom::combinator::map;
    preceded(
        sp,
        alt((
            map(tag("unbounded"), |_| Bound::UnBounded),
            map(preceded(tag("in:"), not_sp), |v| {
                Bound::Include(v.to_string())
            }),
            map(preceded(tag("ex:"), not_sp), |v| {
                Bound::Exclude(v.to_string())
            }),
        )),
    )(i)
}

fn scan<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<'a, (Bound, Bound), E> {
    context("scan", pair(bound, bound))(i)
}

fn keys<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<'a, (Bound, Bound), E> {
    context("keys", pair(bound, bound))(i)
}

fn show<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<'a, ShowItem, E> {
    use nom::combinator::map;
    preceded(
        sp,
        alt((
            map(tag("memtable"), |_| ShowItem::MemTable),
            map(tag("sstable"), |_| ShowItem::SSTable),
            map(tag("level"), |_| ShowItem::Level),
            map(tag("options"), |_| ShowItem::Options),
            map(tag("files"), |_| ShowItem::Files),
        )),
    )(i)
}

fn key<'a, E: ParseError<&'a str> + ContextError<&'a str>>(i: &'a str) -> IResult<'a, String, E> {
    use nom::combinator::map;
    preceded(sp, map(not_sp, |v| v.to_string()))(i)
}

fn channel_pub<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<'a, (String, Vec<String>), E> {
    pair(key, many0(key))(i)
}

fn channel<'a, E: ParseError<&'a str> + ContextError<&'a str>>(
    i: &'a str,
) -> IResult<'a, ChannelOp, E> {
    use nom::combinator::map;
    preceded(
        sp,
        alt((
            map(tag("show"), |_| ChannelOp::Show),
            preceded(
                tag("add"),
                preceded(sp, map(not_sp, |v| ChannelOp::Add(v.to_string()))),
            ),
            map(preceded(tag("pub"), channel_pub), |(key, values)| {
                ChannelOp::Publish(key, values)
            }),
            map(preceded(tag("sub"), key), |key| {
                ChannelOp::Subcribe(false, key)
            }),
            map(preceded(tag("prevmsg-sub"), key), |key| {
                ChannelOp::Subcribe(true, key)
            }),
        )),
    )(i)
}

macro_rules! nom_get_with_report {
    ($func:ident, $input:expr, $op:expr) => {
        match $func::<VerboseError<&str>>($input) {
            Ok(o) => o,
            Result::Err(e) => {
                let err = anyhow!("verbose errors - `{}`:\n{}", $op, e);
                // print error message
                match e {
                    Err::Error(e) | Err::Failure(e) => {
                        eprintln!("verbose errors - `{}`:\n{}", $op, convert_error($input, e));
                    }
                    _ => {}
                }
                return Err(err);
            }
        }
    };
}

fn token_to_basic_op(token: Token, i: &str) -> anyhow::Result<BasicOp> {
    match token {
        Token::Put => {
            let (_, (key, value)) = nom_get_with_report!(put, i, "put");
            Ok(BasicOp::Put(key, value))
        }
        Token::Delete => {
            let (_, key) = nom_get_with_report!(delete, i, "delete");
            Ok(BasicOp::Delete(key))
        }
        Token::Get => {
            let (_, key) = nom_get_with_report!(get, i, "get");
            Ok(BasicOp::Get(key))
        }
        Token::Fill => {
            let (_, pairs) = nom_get_with_report!(fill, i, "fill");
            Ok(BasicOp::Fill(pairs))
        }
        Token::Scan => {
            let (_, (lower, upper)) = nom_get_with_report!(scan, i, "scan");
            Ok(BasicOp::Scan(lower, upper))
        }
        Token::Keys => {
            let (_, (lower, upper)) = nom_get_with_report!(keys, i, "keys");
            Ok(BasicOp::Keys(lower, upper))
        }
        t => Err(anyhow!("unexpected token:{t:?}")),
    }
}

pub fn parse_common_op(i: &str) -> anyhow::Result<CommonOp> {
    let (token, i) = parse_token_with_report(i)?;
    match token {
        Token::Put | Token::Delete | Token::Get | Token::Fill | Token::Scan | Token::Keys => {
            Ok(CommonOp::Basic(token_to_basic_op(token, i)?))
        }
        Token::Show => {
            let (_, item) = nom_get_with_report!(show, i, "show");
            Ok(CommonOp::Show(item))
        }
        Token::Profile => {
            let (token, next) = parse_token_with_report(i)?;
            Ok(CommonOp::Profile(token_to_basic_op(token, next)?))
        }
        Token::Channel => {
            let (_, value) = nom_get_with_report!(channel, i, "channel");
            Ok(CommonOp::Channel(value))
        }
    }
}

#[cfg(test)]
mod tests {
    use nom::error::VerboseError;

    use super::{parse_common_op, put};
    use crate::cli::parser::{bound, channel, fill, get, BasicOp, Bound, ChannelOp, CommonOp};

    #[test]
    fn test_put() {
        let (_, (key, value)) = put::<VerboseError<&str>>(" abc efg").unwrap();
        assert_eq!(key, "abc");
        assert_eq!(value, "efg");
        let (_, (key, value)) = put::<VerboseError<&str>>(" 😂 🤖").unwrap();
        assert_eq!(key, "😂");
        assert_eq!(value, "🤖");
    }

    #[test]
    fn test_get() {
        let (_, key) = get::<VerboseError<&str>>(" abc").unwrap();
        assert_eq!(key, "abc");
    }

    #[test]
    fn test_fill() {
        let (_, key) = fill::<VerboseError<&str>>(" (234,234jlajla) (32323,32443)").unwrap();
        assert_eq!(key[0], ("234".into(), "234jlajla".into()));
        assert_eq!(key[1], ("32323".into(), "32443".into()));
    }

    #[test]
    fn test_bound() {
        let (_, key) = bound::<VerboseError<&str>>(" unbounded").unwrap();
        assert_eq!(key, Bound::UnBounded);
        let (_, key) = bound::<VerboseError<&str>>(" in:abc").unwrap();
        assert_eq!(key, Bound::Include("abc".into()));
        let (_, key) = bound::<VerboseError<&str>>(" ex:abc").unwrap();
        assert_eq!(key, Bound::Exclude("abc".into()));
    }

    #[test]
    fn test_channel() {
        let (_, key) = channel::<VerboseError<&str>>(" add test").unwrap();
        assert_eq!(key, ChannelOp::Add("test".into()));
        let (_, key) = channel::<VerboseError<&str>>(" show ").unwrap();
        assert_eq!(key, ChannelOp::Show);
        let (_, key) = channel::<VerboseError<&str>>(" sub test").unwrap();
        assert_eq!(key, ChannelOp::Subcribe(false, "test".into()));
        let (_, key) = channel::<VerboseError<&str>>(" prevmsg-sub test").unwrap();
        assert_eq!(key, ChannelOp::Subcribe(true, "test".into()));
        let (_, key) = channel::<VerboseError<&str>>(" pub test test0 test1").unwrap();
        assert_eq!(
            key,
            ChannelOp::Publish("test".into(), vec!["test0".into(), "test1".into()])
        );
    }

    #[test]
    fn test_parse_common_op() {
        let op = parse_common_op("put a b").unwrap();
        assert_eq!(op, CommonOp::Basic(BasicOp::Put("a".into(), "b".into())));
        let op = parse_common_op("delete a").unwrap();
        assert_eq!(op, CommonOp::Basic(BasicOp::Delete("a".into())));
        let op = parse_common_op("get a").unwrap();
        assert_eq!(op, CommonOp::Basic(BasicOp::Get("a".into())));
        let op = parse_common_op("fill (1,2) (3,4)").unwrap();
        assert_eq!(
            op,
            CommonOp::Basic(BasicOp::Fill(vec![
                ("1".into(), "2".into()),
                ("3".into(), "4".into())
            ]))
        );
        let op = parse_common_op("scan unbounded in:abc").unwrap();
        assert_eq!(
            op,
            CommonOp::Basic(BasicOp::Scan(
                Bound::UnBounded,
                Bound::Include("abc".into())
            ))
        );
        let op = parse_common_op("keys unbounded in:abc").unwrap();
        assert_eq!(
            op,
            CommonOp::Basic(BasicOp::Keys(
                Bound::UnBounded,
                Bound::Include("abc".into())
            ))
        );
        let op = parse_common_op("keys unbounded in:abc").unwrap();
        assert_eq!(
            op,
            CommonOp::Basic(BasicOp::Keys(
                Bound::UnBounded,
                Bound::Include("abc".into())
            ))
        );
        let op = parse_common_op("show memtable").unwrap();
        assert_eq!(op, CommonOp::Show(crate::cli::parser::ShowItem::MemTable));
        let op = parse_common_op("profile keys unbounded in:abc").unwrap();
        assert_eq!(
            op,
            CommonOp::Profile(BasicOp::Keys(
                Bound::UnBounded,
                Bound::Include("abc".into())
            ))
        );
        let op = parse_common_op("channel show").unwrap();
        assert_eq!(op, CommonOp::Channel(ChannelOp::Show));
        let op = parse_common_op("channel add abc").unwrap();
        assert_eq!(op, CommonOp::Channel(ChannelOp::Add("abc".into())));
        let op = parse_common_op("channel pub a 1 2 3").unwrap();
        assert_eq!(
            op,
            CommonOp::Channel(ChannelOp::Publish(
                "a".into(),
                vec!["1".into(), "2".into(), "3".into()]
            ))
        );
        let op = parse_common_op("channel sub a").unwrap();
        assert_eq!(
            op,
            CommonOp::Channel(ChannelOp::Subcribe(false, "a".into()))
        );
        let op = parse_common_op("channel prevmsg-sub a").unwrap();
        assert_eq!(op, CommonOp::Channel(ChannelOp::Subcribe(true, "a".into())));
    }
}
