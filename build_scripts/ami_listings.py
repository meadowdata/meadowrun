# generated via get_amis_for_regions. Got the initial AMI id in a single region by using
# the console and looking for "Ubuntu Server 20.04 LTS (HVM), SSD Volume Type" under
# quickstart AMIs
VANILLA_UBUNTU_AMIS = {
    "us-east-2": "ami-0960ab670c8bb45f3",
    "us-east-1": "ami-08d4ac5b634553e16",
    "us-west-1": "ami-01154c8b2e9a14885",
    "us-west-2": "ami-0ddf424f81ddb0720",
    "eu-central-1": "ami-0c9354388bb36c088",
    "eu-west-1": "ami-0d2a4a5d69e46ea0b",
    "eu-west-2": "ami-0bd2099338bc55e6d",
    "eu-west-3": "ami-0f7559f51d3a22167",
    "eu-north-1": "ami-012ae45a4a2d92750",
}

BASE_AMIS = {
    "plain": {
        "us-east-2": "ami-0147a27e9e7c91681",
        "us-east-1": "ami-0493f8cc622a17594",
        "us-west-1": "ami-0599f4bb79d709da3",
        "us-west-2": "ami-07fe9668a4030438c",
        "eu-central-1": "ami-0501a522734e3c811",
        "eu-west-1": "ami-05ecb8a0ef47f6b2c",
        "eu-west-2": "ami-0a0c2a5e70f3046a7",
        "eu-west-3": "ami-0f34abf6119363854",
        "eu-north-1": "ami-083a6b67a11ede550",
    },
    "cuda": {
        "us-east-2": "ami-00fd4b9dd94bbb68b",
        "us-east-1": "ami-0c5c6919b7e205fb2",
        "us-west-1": "ami-09f73abffbd956cea",
        "us-west-2": "ami-017c9334f5411ffda",
        "eu-central-1": "ami-016b190d1dd39abed",
        "eu-west-1": "ami-08e58ba1b815bea78",
        "eu-west-2": "ami-0f4a6fe2237d109a2",
        "eu-west-3": "ami-014850847dbc03893",
        "eu-north-1": "ami-0cf21298737c6114f",
    },
}
