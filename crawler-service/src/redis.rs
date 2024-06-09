use deadpool_redis::redis::ToRedisArgs;

pub enum Key<'a> {
    Robots(&'a str),
}

impl<'a> ToRedisArgs for Key<'a> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + deadpool_redis::redis::RedisWrite,
    {
        match self {
            Key::Robots(domain) => out.write_arg_fmt(format!("robots:{domain}")),
        }
    }
}
