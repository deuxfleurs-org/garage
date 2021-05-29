# Configuring a real-world Garage deployment

To run Garage in cluster mode, we recommend having at least 3 nodes.
This will allow you to setup Garage for three-way replication of your data,
the safest and most available mode avaialble.

## Generating a TLS Certificate

You first need to generate TLS certificates to encrypt traffic between Garage nodes
(reffered to as RPC traffic).

To generate your TLS certificates, run on your machine:

```
wget https://git.deuxfleurs.fr/Deuxfleurs/garage/raw/branch/main/genkeys.sh
chmod +x genkeys.sh
./genkeys.sh
```

It will creates a folder named `pki/` containing the keys that you will used for the cluster.

## Real-world deployment

To run a real-world deployment, make sure you the following conditions are met:

- You have at least three machines with sufficient storage space available

- Each machine has a public IP address which is reachable by other machines.
  Running behind a NAT is possible, but having several Garage nodes behind a single NAT
  is slightly more involved as each will have to have a different RPC port number
  (the local port number of a node must be the same as the port number exposed publicly
  by the NAT).

- Ideally, each machine should have a SSD available in addition to the HDD you are dedicating
  to Garage. This will allow for faster access to metadata and has the potential
  to drastically reduce Garage's response times.

Before deploying garage on your infrastructure, you must inventory your machines.
For our example, we will suppose the following infrastructure with IPv6 connectivity:

| Location | Name    | IP Address | Disk Space |
|----------|---------|------------|------------|
| Paris    | Mercury | fc00:1::1  | 1 To       |
| Paris    | Venus   | fc00:1::2  | 2 To       |
| London   | Earth   | fc00:B::1  | 2 To       |
| Brussels | Mars    | fc00:F::1  | 1.5 To     |


On each machine, we will have a similar setup,
especially you must consider the following folders/files:

  - `/etc/garage/garage.toml`: Garage daemon's configuration (see below)
  - `/etc/garage/pki/`: Folder containing Garage certificates, must be generated on your computer and copied on the servers
  - `/var/lib/garage/meta/`: Folder containing Garage's metadata, put this folder on a SSD if possible
  - `/var/lib/garage/data/`: Folder containing Garage's data, this folder will grows and must be on a large storage, possibly big HDDs.
  - `/etc/systemd/system/garage.service`: Service file to start garage at boot automatically (defined below, not required if you use docker)

A valid `/etc/garage/garage.toml` for our cluster would be:

```toml
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"

replication_mode = "3"

rpc_bind_addr = "[::]:3901"

bootstrap_peers = [
  "[fc00:1::1]:3901",
  "[fc00:1::2]:3901",
  "[fc00:B::1]:3901",
  "[fc00:F::1]:3901",
]

[rpc_tls]
ca_cert = "/etc/garage/pki/garage-ca.crt"
node_cert = "/etc/garage/pki/garage.crt"
node_key = "/etc/garage/pki/garage.key"

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"

[s3_web]
bind_addr = "[::]:3902"
root_domain = ".web.garage"
index = "index.html"
```

Please make sure to change `bootstrap_peers` to **your** IP addresses!

Check the [configuration file reference documentation](../reference_manual/configuration.md)
to learn more about all available configuration options.

### For docker users

On each machine, you can run the daemon with:

```bash
docker run \
  -d \
  --name garaged \
  --restart always \
  --network host \
  -v /etc/garage/pki:/etc/garage/pki \
  -v /etc/garage/garage.toml:/garage/garage.toml \
  -v /var/lib/garage/meta:/var/lib/garage/meta \
  -v /var/lib/garage/data:/var/lib/garage/data \
  lxpz/garage_amd64:v0.3.0
```

It should be restart automatically at each reboot.
Please note that we use host networking as otherwise Docker containers
can not communicate with IPv6.

Upgrading between Garage versions should be supported transparently,
but please check the relase notes before doing so!
To upgrade, simply stop and remove this container and
start again the command with a new version of garage.

### For systemd/raw binary users

Create a file named `/etc/systemd/system/garage.service`:

```toml
[Unit]
Description=Garage Data Store
After=network-online.target
Wants=network-online.target

[Service]
Environment='RUST_LOG=garage=info' 'RUST_BACKTRACE=1'
ExecStart=/usr/local/bin/garage server -c /etc/garage/garage.toml

[Install]
WantedBy=multi-user.target
```

To start the service then automatically enable it at boot:

```bash
sudo systemctl start garage
sudo systemctl enable garage
```

To see if the service is running and to browse its logs:

```bash
sudo systemctl status garage
sudo journalctl -u garage
```

If you want to modify the service file, do not forget to run `systemctl daemon-reload`
to inform `systemd` of your modifications.
