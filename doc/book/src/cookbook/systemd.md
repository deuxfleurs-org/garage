# Starting Garage with systemd instead of Docker

NOTE: This guide is incomplete. Typicall you would also want to create a separate
Unix user to run Garage.

Make sure you have the Garage binary installed on your system (see [quick start](../quick_start/index.md)), e.g. at `/usr/local/bin/garage`.

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
