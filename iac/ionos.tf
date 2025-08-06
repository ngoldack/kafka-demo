# Complete example

resource "ionoscloud_datacenter" "example" {
  name     = "example-kafka-datacenter"
  location = "de/fra"
}

resource "ionoscloud_lan" "example" {
  datacenter_id = ionoscloud_datacenter.example.id
  public        = false
  name          = "example-kafka-lan"
}

resource "ionoscloud_server" "example" {
  name              = "example-kafka-server"
  datacenter_id     = ionoscloud_datacenter.example.id
  cores             = 1
  ram               = 2 * 1024
  availability_zone = "AUTO"
  image_name = "ubuntu:latest" # alias name
  image_password    = random_password.password.result
  volume {
    name      = "example-kafka-volume"
    size      = 6
    disk_type = "SSD Standard"
  }
  nic {
    lan  = ionoscloud_lan.example.id
    name = "example-kafka-nic"
    dhcp = true
  }
}

resource "random_password" "password" {
  length  = 16
  special = false
}

locals {
  prefix = format("%s/%s", ionoscloud_server.example.nic[0].ips[0], "24")
  server_net_index              = split(".", ionoscloud_server.example.nic[0].ips[0])[3]
  kafka_cluster_broker_ips      = [
    for i in range(local.server_net_index + 1, local.server_net_index + 4) :cidrhost(local.prefix, i)
  ]
  kafka_cluster_broker_ips_cidr = [for ip in local.kafka_cluster_broker_ips : format("%s/%s", ip, "24")]
}

resource "ionoscloud_kafka_cluster" "example" {
  name     = "example-kafka-cluster"
  location = ionoscloud_datacenter.example.location
  version  = "3.9.0"
  size     = "XS"
  connections {
    datacenter_id = ionoscloud_datacenter.example.id
    lan_id = ionoscloud_lan.example.id
    broker_addresses = local.kafka_cluster_broker_ips_cidr
  }
}