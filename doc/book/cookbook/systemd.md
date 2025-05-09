+++
title = "Starting Garage with systemd"
weight = 15
+++

We make some assumptions for this systemd deployment. 

  - Your garage binary is located at `/usr/local/bin/garage`.

  - Your configuration file is located at `/etc/garage.toml`.

  - Your `garage.toml` must be set with  `metadata_dir=/var/lib/garage/meta` and `data_dir=/var/lib/garage/data`. This is mandatory to use `systemd` hardening feature [Dynamic User](https://0pointer.net/blog/dynamic-users-with-systemd.html). Note that in your host filesystem, Garage data will be held in `/var/lib/private/garage`.



Create a file named `/etc/systemd/system/garage.service`:

```toml
[Unit]
Description=Garage Data Store
After=network-online.target
Wants=network-online.target

[Service]
Environment='RUST_LOG=garage=info' 'RUST_BACKTRACE=1'
ExecStart=/usr/local/bin/garage server
StateDirectory=garage
DynamicUser=true
ProtectHome=true
NoNewPrivileges=true
LimitNOFILE=42000

[Install]
WantedBy=multi-user.target
```

**A note on hardening:** Garage will be run as a non privileged user, its user
id is dynamically allocated by systemd (set with `DynamicUser=true`). It cannot
access (read or write) home folders (`/home`, `/root` and `/run/user`), the
rest of the filesystem can only be read but not written, only the path seen as
`/var/lib/garage` is writable as seen by the service. Additionnaly, the process
can not gain new privileges over time.

For this to work correctly, your `garage.toml` must be set with
`metadata_dir=/var/lib/garage/meta` and `data_dir=/var/lib/garage/data`. This
is mandatory to use the DynamicUser hardening feature of systemd, which
autocreates these directories as virtual mapping. If the directory
`/var/lib/garage` already exists before starting the server for the first time,
the systemd service might not start correctly.  Note that in your host
filesystem, Garage data will be held in `/var/lib/private/garage`.

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
