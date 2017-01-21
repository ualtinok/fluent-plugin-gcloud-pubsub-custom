## ChangeLog

### Release 0.4.0 - 2017/01/21

- Input plugin
  - Add feature to use record key as tag

### Release 0.3.4 - 2017/01/03

- Output plugin
  - Rescue 50x errors
- Input plugin
  - Guard emit to be called with multi-threading
  - Rescue 50x errors
  - Enabled to select whether to raise an exception if message processing failed

### Release 0.3.3 - 2016/12/03

- Input plugin
  - Fix undefined variable error

### Release 0.3.2 - 2016/11/13

- Add plugin param desc
- Input plugin
  - Improve handling to acknowledge messages

### Release 0.3.1 - 2016/11/03

- Output plugin
  - Improve error handling

### Release 0.3.0 - 2016/10/30

- Bump up google-cloud-pubsub to v0.21
- Input plugin
  - Add multithreaded pulling feature

### Release 0.2.0 - 2016/10/15

- Input plugin
  - Add HTTP RPC feature

### Release 0.1.4 - 2016/09/19

- Input plugin
  - `pull_interval` can be specified float value
  - `topic` must be specified

### Release 0.1.3 - 2016/09/17

- Input plugin
  - Fix error handling and add debug logging

### Release 0.1.2 - 2016/09/11

- Output plugin
  - Change default max message size and add debug message

### Release 0.1.1 - 2016/08/27

- Bump up google-cloud-pubsub (gcloud-ruby) to 0.20

### Release 0.1.0 - 2016/08/22

- Use formatter / parser plugin and add format configuration
- Bump up gcloud-ruby to 0.12
- Remove dependency on lightening buffer
- Fix error caused by Pub/Sub quotas
