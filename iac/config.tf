terraform {
  required_providers {
    ionoscloud = {
      source = "ionos-cloud/ionoscloud"
      version = "6.7.12"
    }
  }
}

provider ionoscloud {
}