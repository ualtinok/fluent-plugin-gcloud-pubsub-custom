require 'net/http'

require_relative "../test_helper"

class GcloudPubSubInputTest < Test::Unit::TestCase
  CONFIG = %[
      tag test
      project project-test
      topic topic-test
      subscription subscription-test
      key key-test
      format json
  ]

  DEFAULT_HOST = '127.0.0.1'
  DEFAULT_PORT = 24680

  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::GcloudPubSubInput).configure(conf)
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
      assert_equal('json', d.instance.format)
      assert_equal(false, d.instance.enable_rpc)
      assert_equal('0.0.0.0', d.instance.rpc_bind)
      assert_equal(24680, d.instance.rpc_port)
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

    setup do
      @subscriber = mock!
      @topic_mock = mock!.subscription('subscription-test') { @subscriber }
      @pubsub_mock = mock!.topic('topic-test') { @topic_mock }
      stub(Google::Cloud::Pubsub).new { @pubsub_mock }
    end

    test 'empty' do
      @subscriber.pull(immediate: true, max: 100).once { [] }
      @subscriber.acknowledge.times(0)

      d = create_driver
      d.run {
        # d.run sleeps 0.5 sec
      }

      assert_equal(true, d.emits.empty?)
    end

    test 'simple' do
      messages = Array.new(1, DummyMessage.new)
      @subscriber.pull(immediate: true, max: 100).once { messages }
      @subscriber.acknowledge(messages).once

      d = create_driver
      d.run {
        # d.run sleeps 0.5 sec
      }
      emits = d.emits

      assert_equal(1, emits.length)
      emits.each do |tag, time, record|
        assert_equal("test", tag)
        assert_equal({"foo" => "bar"}, record)
      end
    end

    test 'invalid messages' do
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

      messages = Array.new(1, DummyInvalidMessage.new)
      @subscriber.pull(immediate: true, max: 100).once { messages }
      @subscriber.acknowledge.times(0)

      d = create_driver
      d.run {
        # d.run sleeps 0.5 sec
      }
      assert_equal(true, d.emits.empty?)
    end

    test 'retry if raised error' do
      class UnknownError < StandardError
      end
      @subscriber.pull(immediate: true, max: 100).twice { raise UnknownError.new('test') }
      @subscriber.acknowledge.times(0)

      d = create_driver(CONFIG + 'pull_interval 0.5')
      d.run {
        sleep 0.1 # + 0.5s
      }

      assert_equal(0.5, d.instance.pull_interval)
      assert_equal(true, d.emits.empty?)
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
      emits = d.emits

      assert_equal(1, emits.length)
      assert_equal(true, d.instance.instance_variable_get(:@stop_pull))

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

      d.run {
        http_get('/api/in_gcloud_pubsub/pull/start')
        sleep 0.75
        # d.run sleeps 0.5 sec
      }
      emits = d.emits

      assert_equal(true, emits.length > 0)
      assert_equal(false, d.instance.instance_variable_get(:@stop_pull))

      emits.each do |tag, time, record|
        assert_equal("test", tag)
        assert_equal({"foo" => "bar"}, record)
      end
    end
  end
end
