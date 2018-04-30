# NATS KeyValue Store (NKS)

The NATS KeyValue Store is a simple key value store using NATS that subscribes to all messages with a server-specified prefix.
Tokens following the prefix portion of the subject are used as a key to perform lookups, sets or deletes
on the keyvalue store. Effectively, NKS stores a copy of the last message published using a specific
subject.

To read values, a NATS client simply publishes a request message with the specified prefix. Whatever
value is stored in NKS is returned.

If a message is published without a payload, the operation is understood to be a delete of the key.

NKS supports an embedded mode, in which it runs an embedded NATS server. In the case of embedding
no prefix is necessary, in which case all subjects except those starting with `_INBOX.` are
deemed to be keys.

Currently the key value store used is [moss](https://github.com/couchbase/moss).


## Options

Currently NKS provides the following options:

```
  --prefix string
        keystore prefix (default "keystore")
  -d string
        datadir (default "/tmp")
  -e    Embed gnatsd
  -h string
        server host (default "localhost")
  -p int
        Embedded server port (default 4222)
```

### Starting the server
```
malaga:~/go/src/github.com/aricart/nks [1631:2]% nks -d /tmp/foo -p 4222 -e
Monitoring http://localhost:6619/debug/vars
Starting gnatsd
Connected [nats://localhost:4222]
Listening for keystore requests on >
```


### Sample session
```
malaga:~ [1632]% nats-pub hello world
Published [hello] : 'world'
malaga:~ [1633]% nats-req hello ""
Published [hello] : ''
Received [_INBOX.HY407EBKIAvPCDfNwLPmKk] : 'world'
malaga:~ [1634]%
```

## To Do

- Support TLS
- Implement an idle deadline to force a flushing to disk
- Need built-in metric for iops

## Notes

Lookup performance is about 40k lookups/sec

