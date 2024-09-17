pub enum Key<'a> {
    Robots(&'a str),
    Cooldown(&'a str),
}

impl<'a> redis::ToRedisArgs for Key<'a> {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        match self {
            Key::Robots(domain) => out.write_arg_fmt(format!("r:{domain}")),
            Key::Cooldown(domain) => out.write_arg_fmt(format!("c:{domain}")),
        }
    }
}
