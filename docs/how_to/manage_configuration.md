# Manage configuration

Configuration is managed through `meadowrun-manage-ec2 config`.

## Generate a default configuration file

Use:

```bash
meadowrun-manage-ec2 config --get default
```

## Download the current configuration file

Use:

```bash
meadowrun-manage-ec2 config --get current
```

It's possible there's no configuration set yet.

## Upload a configuration file

Use:

```bash
meadowrun-manage-ec2 config --set custom_config.py
```

Don't forget to replace `custom_config.py` with the name and path to the file you want to upload.