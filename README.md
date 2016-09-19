# fluent-plugin-gcloud-pubsub-custom

[![Build Status](https://travis-ci.org/mia-0032/fluent-plugin-gcloud-pubsub-custom.svg?branch=master)](https://travis-ci.org/mia-0032/fluent-plugin-gcloud-pubsub-custom)
[![Gem Version](https://badge.fury.io/rb/fluent-plugin-gcloud-pubsub-custom.svg)](http://badge.fury.io/rb/fluent-plugin-gcloud-pubsub-custom)

This plugin is forked from https://github.com/mdoi/fluent-plugin-gcloud-pubsub

## Overview

[Cloud Pub/Sub](https://cloud.google.com/pubsub/) Input/Output(BufferedOutput) plugin for [Fluentd](http://www.fluentd.org/) with [gcloud](https://googlecloudplatform.github.io/gcloud-ruby/) gem

- [Publish](https://googlecloudplatform.github.io/gcloud-ruby/docs/v0.12.2/Gcloud/Pubsub/Topic.html#publish-instance_method) messages to Cloud Pub/Sub
- [Pull](https://googlecloudplatform.github.io/gcloud-ruby/docs/v0.12.2/Gcloud/Pubsub/Subscription.html#pull-instance_method) messages from Cloud Pub/Sub

## Preparation

- Create a project on Google Developer Console
- Add a topic of Cloud Pub/Sub to the project
- Add a pull style subscription to the topic
- Download your credential (json) or [set scope on GCE instance](https://cloud.google.com/compute/docs/api/how-tos/authorization)

## Installation

Install by gem:

```shell
$ gem install fluent-plugin-gcloud-pubsub-custom
```

## Configuration

### Publish messages

Use `gcloud_pubsub` output plugin.

```
<match example.publish>
  @type gcloud_pubsub
  project <YOUR PROJECT>
  key <YOUR KEY>
  topic <YOUR TOPIC>
  autocreate_topic false
  max_messages 1000
  max_total_size 9800000
  buffer_type file
  buffer_path /path/to/your/buffer
  flush_interval 1s
  try_flush_interval 0.1
  format json
</match>
```

- `project` (optional)
  - Set your GCP project
  - Running fluentd on GCP, you don't have to specify.
  - You can also use environment variable such as `GCLOUD_PROJECT`.
- `key` (optional)
  - Set your credential file path.
  - Running fluentd on GCP, you can use scope instead of specifying this.
  - You can also use environment variable such as `GCLOUD_KEYFILE`.
- `topic` (required)
  - Set topic name to publish.
- `autocreate_topic` (optional, default: `false`)
  - If set to `true`, specified topic will be created when it doesn't exist.
- `max_messages` (optional, default: `1000`)
  - Publishing messages count per request to Cloud Pub/Sub.
    - See https://cloud.google.com/pubsub/quotas#other_limits
- `max_total_size` (optional, default: `9800000` = `9.8MB`)
  - Publishing messages bytesize per request to Cloud Pub/Sub. This parameter affects only message size. You should specify a little smaller value than quota.
    - See https://cloud.google.com/pubsub/quotas#other_limits
- `buffer_type`, `buffer_path`, `flush_interval`, `try_flush_interval`
  - These are fluentd buffer configuration. See http://docs.fluentd.org/articles/buffer-plugin-overview
- `format` (optional, default: `json`)
  - Set output format. See http://docs.fluentd.org/articles/out_file#format

### Pull messages

Use `gcloud_pubsub` input plugin.

```
<source>
  @type gcloud_pubsub
  tag example.pull
  project <YOUR PROJECT>
  key <YOUR KEY>
  topic <YOUR TOPIC>
  subscription <YOUR SUBSCRIPTION>
  max_messages 1000
  return_immediately true
  pull_interval 0.5
  format json
</source>
```

- `tag` (required)
  - Set tag of messages.
- `project` (optional)
  - Set your GCP project
  - Running fluentd on GCP, you don't have to specify.
  - You can also use environment variable such as `GCLOUD_PROJECT`.
- `key` (optional)
  - Set your credential file path.
  - Running fluentd on GCP, you can use scope instead of specifying this.
  - You can also use environment variable such as `GCLOUD_KEYFILE`.
- `topic` (required)
  - Set topic name to pull.
- `subscription` (required)
  - Set subscription name to pull.
- `max_messages` (optional, default: `100`)
  - See maxMessages on https://cloud.google.com/pubsub/subscriber#receiving-pull-messages
- `return_immediately` (optional, default: `true`)
  - See returnImmediately on https://cloud.google.com/pubsub/subscriber#receiving-pull-messages
  - If `return_immediately` is `false`, this plugin ignore `pull_interval`.
- `pull_interval` (optional, default: `5.0`)
  - Pulling messages by intervals of specified seconds.
- `format` (optional, default: `json`)
  - Set input format. See format section in http://docs.fluentd.org/articles/in_tail

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## TODO

- Add `tag` attribute in output plugin and use `tag` attribute as tag in input plugin.
- Send ack after other output plugin committed (if possible).

## Authors

- [@mdoi](https://github.com/mdoi) : First author
- [@mia-0032](https://github.com/mia-0032) : This version author
