require 'net/http'
require 'json'

require_relative "../test_helper"
require "fluent/test/driver/input"

class GcloudPubSubInputTest < Test::Unit::TestCase
  CONFIG = %[
      tag test
      project project-test
      topic topic-test
      subscription subscription-test
      key key-test
      format json
      json_parser yajl
  ]

  DEFAULT_HOST = '127.0.0.1'
  DEFAULT_PORT = 24680

  class DummyInvalidMsgData
    def data
      return 'foo:bar'
    end
  end
  class DummyInvalidMessage
    def message
      DummyInvalidMsgData.new
    end
  end

  def create_driver(conf=CONFIG)
    Fluent::Test::Driver::Input.new(Fluent::Plugin::GcloudPubSubInput).configure(conf)
  end

  def http_get(path)
    http = Net::HTTP.new(DEFAULT_HOST, DEFAULT_PORT)
    req = Net::HTTP::Get.new(path, {'Content-Type' => 'application/x-www-form-urlencoded'})
    http.request(req)
  end

  setup do
    Fluent::Test.setup
  end

  sub_test_case 'configure' do
    test 'all params are configured' do
      d = create_driver(%[
        tag test
        project project-test
        topic topic-test
        subscription subscription-test
        key key-test
        max_messages 1000
        return_immediately true
        pull_interval 2
        pull_threads 3
        format ltsv
        enable_rpc true
        rpc_bind 127.0.0.1
        rpc_port 24681
      ])

      assert_equal('test', d.instance.tag)
      assert_equal('project-test', d.instance.project)
      assert_equal('topic-test', d.instance.topic)
      assert_equal('subscription-test', d.instance.subscription)
      assert_equal('key-test', d.instance.key)
      assert_equal(2.0, d.instance.pull_interval)
      assert_equal(1000, d.instance.max_messages)
      assert_equal(true, d.instance.return_immediately)
      assert_equal(3, d.instance.pull_threads)
      assert_equal('ltsv', d.instance.format)
      assert_equal(true, d.instance.enable_rpc)
      assert_equal('127.0.0.1', d.instance.rpc_bind)
      assert_equal(24681, d.instance.rpc_port)
    end

    test 'default values are configured' do
      d = create_driver
      assert_equal(5.0, d.instance.pull_interval)
      assert_equal(100, d.instance.max_messages)
      assert_equal(true, d.instance.return_immediately)
      assert_equal(1, d.instance.pull_threads)
      assert_equal('json', d.instance.format)
      assert_equal(false, d.instance.enable_rpc)
      assert_equal('0.0.0.0', d.instance.rpc_bind)
      assert_equal(24680, d.instance.rpc_port)
    end
  end

  sub_test_case 'start' do
    setup do
      @topic_mock = mock!
      @pubsub_mock = mock!.topic('topic-test').at_least(1) { @topic_mock }
      stub(Google::Cloud::Pubsub).new { @pubsub_mock }
    end

    test '40x error occurred on connecting to Pub/Sub' do
      @topic_mock.subscription('subscription-test').once do
        raise Google::Cloud::NotFoundError.new('TEST')
      end

      d = create_driver
      assert_raise Google::Cloud::NotFoundError do
        d.run {}
      end
    end

    test '50x error occurred on connecting to Pub/Sub' do
      @topic_mock.subscription('subscription-test').once do
        raise Google::Cloud::UnavailableError.new('TEST')
      end

      d = create_driver
      assert_raise Google::Cloud::UnavailableError do
        d.run {}
      end
    end

    test 'subscription is nil' do
      @topic_mock.subscription('subscription-test').once { nil }

      d = create_driver
      assert_raise Fluent::GcloudPubSub::Error do
        d.run {}
      end
    end
  end

  sub_test_case 'emit' do
    class DummyMsgData
      def data
        return '{"foo": "bar"}'
      end
    end
    class DummyMessage
      def message
        DummyMsgData.new
      end
    end

    class DummyMsgDataWithTagKey
      def initialize(tag)
        @tag = tag
      end
      def data
        return '{"foo": "bar", "test_tag_key": "' + @tag + '"}'
      end
    end
    class DummyMessageWithTagKey
      def initialize(tag)
        @tag = tag
      end
      def message
        DummyMsgDataWithTagKey.new @tag
      end
    end

    setup do
      @subscriber = mock!
      @topic_mock = mock!.subscription('subscription-test') { @subscriber }
      @pubsub_mock = mock!.topic('topic-test') { @topic_mock }
      stub(Google::Cloud::Pubsub).new { @pubsub_mock }
    end

    test 'empty' do
      @subscriber.pull(immediate: true, max: 100).at_least(1) { [] }
      @subscriber.acknowledge.times(0)

      d = create_driver
      d.run(expect_emits: 1, timeout: 3)

      assert_true d.events.empty?
    end

    test 'simple' do
      messages = Array.new(1, DummyMessage.new)
      @subscriber.pull(immediate: true, max: 100).at_least(1) { messages }
      @subscriber.acknowledge(messages).at_least(1)

      d = create_driver
      d.run(expect_emits: 1, timeout: 3)
      emits = d.events

      assert(1 <= emits.length)
      emits.each do |tag, time, record|
        assert_equal("test", tag)
        assert_equal({"foo" => "bar"}, record)
      end
    end

    test 'multithread' do
      messages = Array.new(1, DummyMessage.new)
      @subscriber.pull(immediate: true, max: 100).at_least(2) { messages }
      @subscriber.acknowledge(messages).at_least(2)

      d = create_driver("#{CONFIG}\npull_threads 2")
      d.run(expect_emits: 2, timeout: 1)
      emits = d.events

      assert(2 <= emits.length)
      emits.each do |tag, time, record|
        assert_equal("test", tag)
        assert_equal({"foo" => "bar"}, record)
      end
    end

    test 'with tag_key' do
      messages = [
        DummyMessageWithTagKey.new('tag1'),
        DummyMessageWithTagKey.new('tag2'),
        DummyMessage.new
      ]
      @subscriber.pull(immediate: true, max: 100).at_least(1) { messages }
      @subscriber.acknowledge(messages).at_least(1)

      d = create_driver("#{CONFIG}\ntag_key test_tag_key")
      d.run(expect_emits: 1, timeout: 3)
      emits = d.events

      assert(3 <= emits.length)
      # test tag
      assert_equal("tag1", emits[0][0])
      assert_equal("tag2", emits[1][0])
      assert_equal("test", emits[2][0])
      # test record
      emits.each do |tag, time, record|
        assert_equal({"foo" => "bar"}, record)
      end
    end

    test 'invalid messages with parse_error_action exception ' do
      messages = Array.new(1, DummyInvalidMessage.new)
      @subscriber.pull(immediate: true, max: 100).at_least(1) { messages }
      @subscriber.acknowledge.times(0)

      d = create_driver
      d.run(expect_emits: 1, timeout: 3)
      assert_true d.events.empty?
    end

    test 'invalid messages with parse_error_action warning' do
      messages = Array.new(1, DummyInvalidMessage.new)
      @subscriber.pull(immediate: true, max: 100).at_least(1) { messages }
      @subscriber.acknowledge(messages).at_least(1)

      d = create_driver("#{CONFIG}\nparse_error_action warning")
      d.run(expect_emits: 1, timeout: 3)
      assert_true d.events.empty?
    end

    test 'retry if raised error' do
      class UnknownError < StandardError
      end
      @subscriber.pull(immediate: true, max: 100).at_least(2) { raise UnknownError.new('test') }
      @subscriber.acknowledge.times(0)

      d = create_driver(CONFIG + 'pull_interval 0.5')
      d.run(expect_emits: 1, timeout: 0.8)

      assert_equal(0.5, d.instance.pull_interval)
      assert_true d.events.empty?
    end

    test 'retry if raised RetryableError on pull' do
      @subscriber.pull(immediate: true, max: 100).at_least(2) { raise Google::Cloud::UnavailableError.new('TEST') }
      @subscriber.acknowledge.times(0)

      d = create_driver("#{CONFIG}\npull_interval 0.5")
      d.run(expect_emits: 1, timeout: 0.8)

      assert_equal(0.5, d.instance.pull_interval)
      assert_true d.events.empty?
    end

    test 'retry if raised RetryableError on acknowledge' do
      messages = Array.new(1, DummyMessage.new)
      @subscriber.pull(immediate: true, max: 100).at_least(2) { messages }
      @subscriber.acknowledge(messages).twice { raise Google::Cloud::UnavailableError.new('TEST') }

      d = create_driver("#{CONFIG}\npull_interval 0.5")
      d.run(expect_emits: 2, timeout: 3)
      emits = d.events

      # not acknowledged, but already emitted to engine.
      assert(2 <= emits.length)
      emits.each do |tag, time, record|
        assert_equal("test", tag)
        assert_equal({"foo" => "bar"}, record)
      end
    end

    test 'stop by http rpc' do
      messages = Array.new(1, DummyMessage.new)
      @subscriber.pull(immediate: true, max: 100).once { messages }
      @subscriber.acknowledge(messages).once

      d = create_driver("#{CONFIG}\npull_interval 1.0\nenable_rpc true")
      assert_equal(false, d.instance.instance_variable_get(:@stop_pull))

      d.run {
        http_get('/api/in_gcloud_pubsub/pull/stop')
        sleep 0.75
        # d.run sleeps 0.5 sec
      }
      emits = d.events

      assert_equal(1, emits.length)
      assert_true d.instance.instance_variable_get(:@stop_pull)

      emits.each do |tag, time, record|
        assert_equal("test", tag)
        assert_equal({"foo" => "bar"}, record)
      end
    end

    test 'start by http rpc' do
      messages = Array.new(1, DummyMessage.new)
      @subscriber.pull(immediate: true, max: 100).at_least(1) { messages }
      @subscriber.acknowledge(messages).at_least(1)

      d = create_driver("#{CONFIG}\npull_interval 1.0\nenable_rpc true")
      d.instance.stop_pull
      assert_equal(true, d.instance.instance_variable_get(:@stop_pull))

      d.run(expect_emits: 1, timeout: 3) {
        http_get('/api/in_gcloud_pubsub/pull/start')
        sleep 0.75
        # d.run sleeps 0.5 sec
      }
      emits = d.events

      assert_equal(true, emits.length > 0)
      assert_false d.instance.instance_variable_get(:@stop_pull)

      emits.each do |tag, time, record|
        assert_equal("test", tag)
        assert_equal({"foo" => "bar"}, record)
      end
    end

    test 'get status by http rpc when started' do
      d = create_driver("#{CONFIG}\npull_interval 1.0\nenable_rpc true")
      assert_false d.instance.instance_variable_get(:@stop_pull)

      d.run {
        res = http_get('/api/in_gcloud_pubsub/pull/status')
        assert_equal({"ok" => true, "status" => "started"}, JSON.parse(res.body))
      }
    end

    test 'get status by http rpc when stopped' do
      d = create_driver("#{CONFIG}\npull_interval 1.0\nenable_rpc true")
      d.instance.stop_pull
      assert_true d.instance.instance_variable_get(:@stop_pull)

      d.run {
        res = http_get('/api/in_gcloud_pubsub/pull/status')
        assert_equal({"ok" => true, "status" => "stopped"}, JSON.parse(res.body))
      }
    end
  end
end
