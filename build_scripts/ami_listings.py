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
        "us-east-2": "ami-0eed2bad104153216",
        "us-east-1": "ami-0237a465e7f465b10",
        "us-west-1": "ami-0ccdaf675a40e9117",
        "us-west-2": "ami-01428f068e1a56d69",
        "eu-central-1": "ami-09f2794e7d903bcef",
        "eu-west-1": "ami-07c1ea49baf56df87",
        "eu-west-2": "ami-0726d725c53077e8f",
        "eu-west-3": "ami-07e07ed18b2be6885",
        "eu-north-1": "ami-075035b99ea138b74",
    },
    "cuda": {
        "us-east-2": "ami-0a71ec6df86abde74",
        "us-east-1": "ami-0079ddb41f8365264",
        "us-west-1": "ami-008b573d9c68f9e2a",
        "us-west-2": "ami-0ce7eb2c350324d29",
        "eu-central-1": "ami-04682aea45f749afb",
        "eu-west-1": "ami-055d9249476b39cca",
        "eu-west-2": "ami-03f87b8eade45709d",
        "eu-west-3": "ami-04653e6ad61b9b8a5",
        "eu-north-1": "ami-0358b4c743a05cdc4",
    },
}


AMI_SIZES_GB = {"plain": 8, "cuda": 16}
