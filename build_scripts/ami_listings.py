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
        "us-east-2": "ami-0c0e9b2876490a848",
        "us-east-1": "ami-02926e1774240c532",
        "us-west-1": "ami-0676090a7fc8e7a83",
        "us-west-2": "ami-0db5ccf67230b16dd",
        "eu-central-1": "ami-042e250513f73e4c8",
        "eu-west-1": "ami-0972fee81e27b19d0",
        "eu-west-2": "ami-0e1ef164398622295",
        "eu-west-3": "ami-03838e066e91ec758",
        "eu-north-1": "ami-05e71a7f8e72bf481",
    },
    "cuda": {
        "us-east-2": "ami-0b0cfe133703b7017",
        "us-east-1": "ami-09097bb6bb2fc7a01",
        "us-west-1": "ami-01ec7d42be6a73082",
        "us-west-2": "ami-0d4179a49ea001e32",
        "eu-central-1": "ami-06a3533bd5264512d",
        "eu-west-1": "ami-069ba66c5f142d362",
        "eu-west-2": "ami-09b8bcfa50817d43a",
        "eu-west-3": "ami-05595226381771738",
        "eu-north-1": "ami-0232fcf335c875423",
    },
}
