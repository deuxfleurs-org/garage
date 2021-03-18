# Control the daemon

The `garage` binary has two purposes:
  - it acts as a daemon when launched with `garage server ...`
  - it acts as a control tool for the daemon when launched with any other command

In this section, we will see how to use the `garage` binary as a control tool for the daemon we just started.
You first need to get a shell having access to this binary, which depends of your configuration:
  - with `docker-compose`, run `sudo docker-compose exec g1 bash` then `/garage/garage`
  - with `docker`, run `sudo docker exec -ti garaged bash` then `/garage/garage`
  - with `systemd`, simply run `/usr/local/bin/garage` if you followed previous instructions

*You can also install the binary on your machine to remotely control the cluster.*

## Talk to the daemon and create an alias

`garage` requires 4 options to talk with the daemon:

```
--ca-cert <ca-cert>            
--client-cert <client-cert>    
--client-key <client-key>      
-h, --rpc-host <rpc-host>
```

The 3 first ones are certificates and keys needed by TLS, the last one is simply the address of garage's RPC endpoint.
Because we configure garage directly from the server, we do not need to set `--rpc-host`.
To avoid typing the 3 first options each time we want to run a command, we will create an alias.

### `docker-compose` alias

```bash
alias garagectl='/garage/garage \
  --ca-cert /pki/garage-ca.crt \
  --client-cert /pki/garage.crt \
  --client-key /pki/garage.key'
```

### `docker` alias

```bash
alias garagectl='/garage/garage \
  --ca-cert /etc/garage/pki/garage-ca.crt \
  --client-cert /etc/garage/pki/garage.crt \
  --client-key /etc/garage/pki/garage.key'
```


### raw binary alias

```bash
alias garagectl='/usr/local/bin/garage \
  --ca-cert /etc/garage/pki/garage-ca.crt \
  --client-cert /etc/garage/pki/garage.crt \
  --client-key /etc/garage/pki/garage.key'
```

Of course, if your deployment does not match exactly one of this alias, feel free to adapt it to your needs!

## Test the alias

You can test your alias by running a simple command such as:

```
garagectl status
```

You should get something like that as result:

```
Healthy nodes:
2a638ed6c775b69a…	37f0ba978d27	[::ffff:172.20.0.101]:3901	UNCONFIGURED/REMOVED
68143d720f20c89d…	9795a2f7abb5	[::ffff:172.20.0.103]:3901	UNCONFIGURED/REMOVED
8781c50c410a41b3…	758338dde686	[::ffff:172.20.0.102]:3901	UNCONFIGURED/REMOVED
```

...which means that you are ready to configure your cluster!
